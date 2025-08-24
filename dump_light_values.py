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
        try:
            self.sensor = BH1750(bus=bus, addr=addr)
        except Exception as e:
            print(f"Failed to initialize BH1750 sensor: {e}", file=sys.stderr)
            sys.exit(2)
        if filename is None:
            filename = f"light_data_{datetime.now().strftime('%Y%m%d')}.csv"
        self.csv_path = Path(filename)
        self.readings_per_interval = max(1, min(5, int(self.interval / 8) + 1))
        self.sample_delay = 0.1
        self.max_consecutive_errors = 5
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
            print("Statistical analysis enabled (trimmed/median style)")

    def _signal_handler(self, signum, frame):
        print(f"\nReceived signal {signum}, shutting down gracefully...")
        self.stop()

    def _calculate_stats(self, values):
        if not values:
            return None, None, None, None
        vmin = int(round(min(values)))
        vmax = int(round(max(values)))
        median = int(round(statistics.median(values)))
        stdev = int(round(statistics.stdev(values))) if len(values) > 1 else 0
        return vmin, vmax, median, stdev

    def _aggregate_readings(self, readings):
        if not readings:
            return None
        if len(readings) == 1:
            return int(round(readings[0])), [readings[0]]
        m = statistics.median(readings)
        devs = [abs(x - m) for x in readings]
        mad = statistics.median(devs)
        if mad == 0:
            threshold = 2.0
        else:
            threshold = 3.0 * 1.4826 * mad
        filtered = [x for x in readings if abs(x - m) <= threshold]
        if not filtered:
            filtered = readings
        rep = int(round(statistics.median(filtered)))
        return rep, filtered

    def _read_and_log_loop(self):
        consecutive_errors = 0
        next_time = time.time()
        while self.running:
            try:
                samples = []
                for _ in range(self.readings_per_interval):
                    try:
                        lux = self.sensor.read_lux()
                        samples.append(lux)
                        consecutive_errors = 0
                    except Exception as e:
                        consecutive_errors += 1
                        print(f"Sensor read error ({consecutive_errors}/{self.max_consecutive_errors}): {e}", file=sys.stderr)
                        if consecutive_errors >= self.max_consecutive_errors:
                            print("Too many consecutive errors, stopping logger", file=sys.stderr)
                            self.running = False
                            break
                    if len(samples) < self.readings_per_interval:
                        time.sleep(self.sample_delay)
                if not self.running:
                    break
                valid = [s for s in samples if s is not None]
                if valid:
                    agg = self._aggregate_readings(valid)
                    if agg is None:
                        representative = None
                        filtered = []
                    elif isinstance(agg, tuple):
                        representative, filtered = agg
                    else:
                        representative = int(round(agg))
                        filtered = valid
                    if representative is not None:
                        ts = int(time.time())
                        iso = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                        row = [ts, iso, representative]
                        if self.include_stats:
                            stats = self._calculate_stats(filtered)
                            if stats[0] is None:
                                row.extend([representative, representative, representative, 0, len(filtered)])
                            else:
                                vmin, vmax, vmid, vstdev = stats
                                row.extend([vmin, vmax, vmid, vstdev, len(filtered)])
                        with open(self.csv_path, 'a', newline='') as csvfile:
                            writer = csv.writer(csvfile)
                            writer.writerow(row)
                        print(f"{iso} - {representative} lx")
                next_time += self.interval
                sleep_for = next_time - time.time()
                if sleep_for > 0:
                    time.sleep(sleep_for)
                else:
                    next_time = time.time()
            except Exception as e:
                print(f"Logging loop error: {e}", file=sys.stderr)
                time.sleep(1.0)

    def start(self):
        if self.running:
            return
        self.running = True
        print(f"Starting light sensor logging every {self.interval} seconds (readings per interval: {self.readings_per_interval})...")
        print(f"Data file: {self.csv_path}")
        print("Press Ctrl+C to stop")
        worker = threading.Thread(target=self._read_and_log_loop, daemon=True)
        worker.start()
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
    parser.add_argument('-f', '--filename', type=str, help='CSV filename (default: auto-generated with timestamp)')
    parser.add_argument('--addr', type=lambda x: int(x,0), default=0x23, help='I2C address (default: 0x23)')
    parser.add_argument('--bus', type=int, default=1, help='I2C bus number (default: 1)')
    parser.add_argument('--buffer-size', type=int, default=100, help='Internal buffer size (default: 100)')
    parser.add_argument('--no-stats', action='store_true', help='Disable statistical analysis')
    return parser.parse_args()

def main():
    args = parse_csv_args()
    try:
        logger = CSVLightLogger(filename=args.filename, interval=logging_interval, bus=args.bus, addr=args.addr, buffer_size=args.buffer_size, include_stats=not args.no_stats)
        logger.start()
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()

