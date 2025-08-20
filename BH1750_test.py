#!/usr/bin/env python3
import time
import argparse
import sys
from smbus2 import SMBus, i2c_msg

POWER_ON = 0x01
RESET = 0x07
CONT_H_RES_MODE = 0x10
ONE_TIME_H_RES_MODE = 0x20

class BH1750:
    def __init__(self, bus=1, addr=0x23, measurement_mode=CONT_H_RES_MODE):
        self.busnum = bus
        self.addr = addr
        self.mode = measurement_mode
        self.bus = SMBus(self.busnum)
        self._power_on()
        self._reset()
        self.set_mode(self.mode)

    def _power_on(self):
        self.bus.write_byte(self.addr, POWER_ON)
        time.sleep(0.01)

    def _reset(self):
        try:
            self.bus.write_byte(self.addr, RESET)
        except OSError:
            pass
        time.sleep(0.01)

    def set_mode(self, mode):
        self.mode = mode
        self.bus.write_byte(self.addr, self.mode)
        time.sleep(0.18)

    def read_raw(self):
        read = i2c_msg.read(self.addr, 2)
        self.bus.i2c_rdwr(read)
        data = list(read)
        if len(data) != 2:
            raise IOError("I2C read returned wrong number of bytes")
        return (data[0] << 8) | data[1]

    def read_lux_once(self):
        self.bus.write_byte(self.addr, ONE_TIME_H_RES_MODE)
        time.sleep(0.18)
        raw = self.read_raw()
        return raw / 1.2

    def read_lux(self):
        raw = self.read_raw()
        return raw / 1.2

def parse_args():
    p = argparse.ArgumentParser(description="Read BH1750 on Raspberry Pi (pins 1,3,5,9 -> 3.3V,SDA,SCL,GND)")
    p.add_argument("--addr", type=lambda x: int(x,0), default=0x23)
    p.add_argument("--bus", type=int, default=1)
    p.add_argument("--once", action="store_true")
    p.add_argument("--interval", type=float, default=1.0)
    p.add_argument("--count", type=int, default=0)
    return p.parse_args()

def main():
    args = parse_args()
    try:
        sensor = BH1750(bus=args.bus, addr=args.addr)
    except Exception as e:
        print("Failed to open I2C bus or find device:", e, file=sys.stderr)
        sys.exit(2)
    if args.once:
        try:
            lux = sensor.read_lux_once()
            print(f"{lux:.2f} lx")
        except Exception as e:
            print("Read failed:", e, file=sys.stderr)
            sys.exit(3)
        return
    count = args.count
    printed = 0
    try:
        while True:
            try:
                lux = sensor.read_lux()
                print(f"{time.strftime('%Y-%m-%d %H:%M:%S')}  {lux:.2f} lx")
            except Exception as e:
                print("Read failed:", e, file=sys.stderr)
            printed += 1
            if count and printed >= count:
                break
            time.sleep(max(0.02, args.interval))
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()

