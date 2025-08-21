import time
import csv
import argparse
import sys
import signal
import threading
from pathlib import Path
from datetime import datetime, timezone
from queue import Queue, Empty
import statistics
import json

logging_interval = 20.0

try:
    from BH1750_test import BH1750, parse_args as original_parse_args
except ImportError:
    print("Error: Could not import BH1750_test.py. Ensure it's in the same directory.", file=sys.stderr)
    sys.exit(1)

class CSVLightLogger:
    def __init__(self, filename=None, interval=None, bus=1, addr=0x23, buffer_size=100, include_stats=True):
        self.interval = logging_interval if interval is None else interval
        self.buffer_size = buffer_size
        self.include_stats = include_stats
        self.running = False
        self.data_queue = Queue()
        self.stats_buffer = []
        try:
            self.sensor = BH1750(bus=bus, addr=addr)
        except Exception as e:
            print(f"Failed to initialize BH1750 sensor: {e}", file=sys.stderr)
            sys.exit(2)
        if filename is None:
            filename = f"light_data_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        self.csv_path = Path(filename)
        self._initialize_csv()
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _initialize_csv(self):
        headers = ['timestamp', 'iso_timestamp', 'lux_value']
        if self.include_stats:
            headers.extend(['min_lux_1min', 'max_lux_1min', 'avg_lux_1min', 'std_lux_1min', 'sample_count'])
        with open(self.csv_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)
        print(f"CSV logging initialized: {self.csv_path}")
        if self.include_stats:
            print("Statistical analysis enabled (1-minute window)")
    
    def _signal_handler(self, signum, frame):
        print(f"\nReceived signal {signum}, shutting down gracefully...")
        self.stop()
    
    def _backup_data(self):
        if self.auto_backup and self.csv_path.exists():
            try:
                import shutil
                shutil.copy2(self.csv_path, self.backup_path)
                print(f"Backup created: {self.backup_path}")
            except Exception as e:
                print(f"Backup failed: {e}", file=sys.stderr)
    
    def _calculate_stats(self, values):
        if not values:
            return None, None, None, None
        return (min(values), max(values), statistics.mean(values), statistics.stdev(values) if len(values) > 1 else 0.0)
    
    def _read_sensor_continuous(self):
        consecutive_errors = 0
        max_errors = 5
        sleep_between_reads = max(0.2, self.interval / 5.0)
        while self.running:
            try:
                lux = self.sensor.read_lux()
                timestamp = time.time()
                self.data_queue.put((timestamp, lux))
                consecutive_errors = 0
                time.sleep(sleep_between_reads)
            except Exception as e:
                consecutive_errors += 1
                print(f"Sensor read error ({consecutive_errors}/{max_errors}): {e}", file=sys.stderr)
                if consecutive_errors >= max_errors:
                    print("Too many consecutive errors, stopping sensor thread", file=sys.stderr)
                    break
                time.sleep(1.0)
    
    def _process_and_log_data(self):
        last_log_time = time.time()
        buffer_samples = []
        while self.running:
            try:
                current_time = time.time()
                while True:
                    try:
                        timestamp, lux = self.data_queue.get_nowait()
                        buffer_samples.append((timestamp, lux))
                    except Empty:
                        break
                if current_time - last_log_time >= self.interval:
                    if buffer_samples:
                        latest_timestamp, latest_lux = buffer_samples[-1]
                        dt = datetime.fromtimestamp(latest_timestamp, tz=timezone.utc)
                        row_data = [int(latest_timestamp), dt.isoformat(), round(latest_lux, 2)]
                        if self.include_stats:
                            lux_values = [lux for _, lux in buffer_samples]
                            min_lux, max_lux, avg_lux, std_lux = self._calculate_stats(lux_values)
                            row_data.extend([
                                round(min_lux, 2) if min_lux is not None else None,
                                round(max_lux, 2) if max_lux is not None else None,
                                round(avg_lux, 2) if avg_lux is not None else None,
                                round(std_lux, 2) if std_lux is not None else None,
                                len(lux_values)])
                        with open(self.csv_path, 'a', newline='') as csvfile:
                            writer = csv.writer(csvfile)
                            writer.writerow(row_data)
                        print(f"{dt.strftime('%Y-%m-%d %H:%M:%S')} - {latest_lux:.2f} lx "
                              f"(samples: {len(buffer_samples)})")
                        buffer_samples.clear()
                    else:
                        print(f"No samples collected in the last {self.interval} seconds")
                    last_log_time = current_time
                time.sleep(0.5)
            except Exception as e:
                print(f"Data processing error: {e}", file=sys.stderr)
                time.sleep(5.0)
    
    def start(self):
        if self.running:
            return
        self.running = True
        print(f"Starting light sensor logging every {self.interval} seconds...")
        print(f"Data file: {self.csv_path}")
        print("Press Ctrl+C to stop")
        sensor_thread = threading.Thread(target=self._read_sensor_continuous, daemon=True)
        sensor_thread.start()
        process_thread = threading.Thread(target=self._process_and_log_data, daemon=True)
        process_thread.start()
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()
    
    def stop(self):
        if not self.running:
            return
        print("Stopping logger...")
        self.running = False
        if self.csv_path.exists():
            try:
                with open(self.csv_path, 'r') as f:
                    reader = csv.reader(f)
                    rows = list(reader)
                    if len(rows) > 1:
                        print(f"Total records logged: {len(rows) - 1}")
            except Exception:
                pass
        print("Logger stopped.")

def parse_csv_args():
    parser = argparse.ArgumentParser(
        description="Log BH1750 light sensor data to CSV file",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                          #Log using logging_interval from top of script
  %(prog)s -f light_data.csv        #Log to specific file
  %(prog)s --no-stats               #Disable statistical analysis
  %(prog)s --addr 0x5c --bus 0      #Use different I2C address/bus
        """)
    parser.add_argument('-f', '--filename', type=str,
                       help='CSV filename (default: auto-generated with timestamp)')
    parser.add_argument('--addr', type=lambda x: int(x,0), default=0x23,
                       help='I2C address (default: 0x23)')
    parser.add_argument('--bus', type=int, default=1,
                       help='I2C bus number (default: 1)')
    parser.add_argument('--buffer-size', type=int, default=100,
                       help='Internal buffer size (default: 100)')
    parser.add_argument('--no-stats', action='store_true',
                       help='Disable statistical analysis')
    return parser.parse_args()

def main():
    args = parse_csv_args()
    try:
        logger = CSVLightLogger(
            filename=args.filename,
            interval=logging_interval,
            bus=args.bus,
            addr=args.addr,
            buffer_size=args.buffer_size,
            include_stats=not args.no_stats
        )
        logger.start()
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()

