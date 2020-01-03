#!/usr/bin/env python
# coding: utf-8
from multiprocessing import Process, cpu_count

from receiver import receiver
from core.config import config


def main():
    for i in range(cpu_count()):
        # Process(target=server, args=(i,)).start()
        Process(target=receiver.run, args=(9000 + i, )).start()

if __name__ == '__main__':
    main()
