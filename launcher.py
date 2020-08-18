#!/usr/bin/env python3
#i-*- encoding: utf-8 -*-

import os
import sys
import time
import errno
import signal
import subprocess

help = """
USAGE:
    python launcher.py <file_to_run> <start | stop | restart | alive>

    file_to_run:: The main script to launch. (/!\ Needs a shebang and executions rights)

    Commands::
        - start:   Start the process and write a '<file_to_run>.pid' file.
        - stop:    Send the signal to end the process gracefully and wait its end.
        - restart: Like stop and start.
        - alive:   Check if the process is alive, work also with nagios

EXEMPLE:
    $ python3 launcher.py model.py start
    $ python3 launcher.py model.py alive
"""

PIDFILE = '{}.pid'
LOGFILE = "/logs/py3_model-0_.log"

def write_pid(pid):
    with open(PIDFILE, 'w') as pidfile:
        pidfile.write(str(pid))

def get_pid():
    with open(PIDFILE, 'r') as pidfile:
        pid = pidfile.read().strip()

    try:
        pid = int(pid)
    except ValueError:
        print('Corrupted pidfile.')
        return None

    return pid

def is_running(pid):

    try:
        os.kill(pid, 0)
    except OSError as err:
        if err.errno == errno.EPERM:
            return True
    else:
        return True

    return False

def nagios(main):

    main = os.path.join(os.getcwd(), main)

    i = len(main) - 2

    while i >= 0 and main[i] != '/': # '/' or '\'
        i -= 1

    log = main[:i] + LOGFILE

    with subprocess.Popen(["tail -n 30 " + log], shell=True, stdout=subprocess.PIPE) as proc:
        tail = proc.stdout.read().decode('utf-8')

    if os.path.isfile(PIDFILE):

        pid = get_pid()

        if pid != None and is_running(pid):

            if "Level:ERROR" in tail:
                x = 1
            elif "Level:CRITICAL" in tail:
                x = 2
            else:
                x = 0
        else:
            x = 2
    else:
        x = 3

    return x, tail

def alive(main):
    x, tail = nagios(main)

    if x == 0:
        print("OK - No recent problems")
    elif x == 1:
        print("WARNING - Some none fatal error has occured")
    elif x == 2:
        print("CRITICAL - Something made the program stop in a non-standard way")
    elif x == 3:
        print("UNKNOWN - The program seems to have ended gracefully")

    if tail:
        print(tail)
    else:
        print("Cannot gather the end of the log")

    sys.exit(x)

def start(main):

    if os.path.isfile(PIDFILE):

        pid = get_pid()

        if pid != None and is_running(pid):
            print("The process is already running.")
            return
        else:
            print("It seems that the last instance did not ended gracefully.")

    print("Starting the process...", end='')

    try:
        sub = subprocess.Popen(os.path.join(os.getcwd(), main))
    except PermissionError:
        print("\nPlease make sure you have execution rights on: {}".format(main))
        return
    except OSError as err:
        if err.errno == errno.ENOEXEC:
            print("\nPlease make sure the file {} have a shebang".format(main))
            return
        else:
            raise err

    i   = 0
    while i < 15:
        if is_running(sub.pid):
            break
        else:
            time.sleep(1)
            print('.', end='')

        i += 1

    print('')

    if i == 15:
        print("The process seems to have crashed at start.")
        return

    print("Started at pid: {}".format(sub.pid))

    write_pid(sub.pid)
