#!/usr/bin/env python
"""
This script will allow for smaller repairs of Cassandra ranges.

#################################################
# success, ring_tokens, error = get_ring_tokens()
# success, host_token, error = get_host_token()
# range_termination = get_range_termination(host_token, ring_tokens)
# steps = 100

# print repr(is_murmur_ring(ring_tokens))
# print repr(get_ring_tokens())
# print repr(get_host_token())
# print repr(get_range_termination(host_token, ring_tokens))
# print repr(get_sub_range_generator(host_token, range_termination, steps).next())
#################################################
"""
from optparse import OptionParser

import logging
import operator
import os
import subprocess
import sys


NODETOOL = None


class CommandResult(object):

    def __init__(self, proc, cmd):
        self.proc = proc
        self.cmd = cmd

    def result(self):
        stdout, stderr = self.proc.communicate()
        return self.proc.returncode == 0, self.proc.returncode, self.cmd, stdout, stderr

    def retry(self):
        logging.debug("retrying command %s..." % self.cmd)
        return run_command(self.cmd)


def lrange(num1, num2=None, step=1):
    op = operator.__le__

    if num2 is None:
        num1, num2 = 0, num1
    if num2 < num1:
        if step > 0:
            num1 = num2
        op = operator.__gt__
    elif step < 0:
        num1 = num2

    while op(num1, num2):
        yield num1
        num1 += step

def run_command(command, *args):
    """Execute a shell command and return the output
    """
    cmd = " ".join([command] + list(args))
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return CommandResult(proc, cmd)

def is_murmur_ring(ring):
    """check whether or not the ring is a Mumur3 ring
    :param ring: ring information
    """
    for i in ring:
        if i < 0:
            return True
    return False

def get_nodetool_cmd():
    return [NODETOOL or "nodetool"]

def get_ring_tokens(host=None):
    """Gets the token information for the ring
    """
    tokens = []
    logging.info("running nodetool ring, this will take a little bit of time")
    cmd = get_nodetool_cmd()
    if host:
        cmd.append("-h {host}".format(host=host))
    cmd.append("ring")
    success, return_code, _, stdout, stderr = run_command(" ".join(cmd)).result()

    if not success:
        return False, [], stderr

    logging.debug("ring tokens found, creating ring token list...")
    for line in stdout.split("\n")[6:]:
        segments = line.split()
        # Filter tokens from joining nodes
        if (len(segments) == 8) and (segments[3] != "Joining"):
            tokens.append(long(segments[-1]))

    return True, sorted(tokens), None

def get_host_tokens(host=None):
    """Gets the tokens ranges for the target host
    :param host: (optional) hostname
    """
    cmd = get_nodetool_cmd()
    if host:
        cmd.append("-h {host}".format(host=host))
    cmd.append("info -T")
    success, return_code, _, stdout, stderr = run_command(" ".join(cmd)).result()
    if not success or stdout.find("Token") == -1:
        logging.error(stdout)
        return False, [], stderr
    token_list = []
    logging.debug("host tokens found, creating host token list...")
    for line in stdout.split("\n"):
        if not line.startswith("Token"): continue
        parts = line.split()
        token_list.append(long(parts[2]))

    return True, token_list, None

def get_range_termination(token, ring):
    """get the last/largest token in the ring
    :param token: Token to start from
    :param ring: All of the tokens allocated in the ring
    :returns: The token that falls immediately after the argument token
    """
    for i in ring:
        if token < i:
            return i
    # token is the largest value in the ring.  Since the rings wrap around,
    # return the first value.
    return ring[0]

def get_sub_range_generator(start, stop, steps=100):
    """Generate $step subranges between $start and $stop
    :param start: beginning token in the range
    :param stop: ending token in the range
    :param steps: number of sub-ranges to create

    There is special-case handling for when there are more steps than there
    are keys in the range: just return the start and stop values.
    """
    if start+steps+1 < stop:
        step_increment = abs(stop - start) / steps
        for i in lrange(start + step_increment, stop + 1, step_increment):
            yield start, i
            start = i
        if start < stop:
            yield start, stop
    else:
        yield start, stop

def repair_range(keyspace, start, end, columnfamily=None, host=None, all_dcs=False):
    """Repair a keyspace/columnfamily between a given token range with nodetool
    :param keyspace: Cassandra keyspace to repair
    :param start: Beginning token in the range to repair
    :param end: Ending token in the range to repair
    :param columnfamily: (optional) Cassandra Columnfamily to repair
    :param host: (optional) Hostname to pass to nodetool
    """
    cmd = get_nodetool_cmd()
    if host:
        cmd.append("-h {host}".format(host=host))
    cmd.append("repair {keyspace}".format(keyspace=keyspace))
    if columnfamily:
        cmd.append(columnfamily)
    if not all_dcs:
        cmd.append("-local")
    cmd.append("-pr -st {start} -et {end}".format(start=start, end=end))
    return run_command(" ".join(cmd))

def setup_logging(loglevel=None):
    """Sets up logging in a syslog format by log level
    """
    log_format = "%(levelname) -10s %(asctime)s %(funcName) -20s line:%(lineno) -5d: %(message)s"
    log_level = loglevel or os.getenv("LOG_LEVEL", "INFO")
    logging.basicConfig(level=logging.getLevelName(log_level), format=log_format)

def format_murmur(num):
    """Format a number for Murmur3
    :param num: Murmr3 number to be formatted
    """
    return "{0:020d}".format(num)

def format_md5(num):
    """Format a number for RandomPartitioner
    :param num: RandomPartitioner number to be formatted
    """
    return "{0:039d}".format(num)

def repair(keyspace, columnfamily=None, host=None, start_steps=100, repair_concurrency=50, all_dcs=False):
    """Repair a keyspace/columnfamily by breaking each token range into $start_steps ranges
    :param keyspace: Cassandra keyspace to repair
    :param columnfamily: Cassandra columnfamily to repair
    :param host: (optional) Hostname to pass to nodetool 
    :param start_steps: Number of sub-ranges to split primary range in to
    """
    success, ring_tokens, error = get_ring_tokens(host)
    if not success:
        logging.error("Error fetching ring tokens: {0}".format(error))
        return False

    success, host_token_list, error = get_host_tokens(host)
    if not success:
        logging.error("Error fetching host token: {0}".format(error))
        return False

    total_tokens = len(host_token_list)
    for token_num, host_token in enumerate(host_token_list):
        steps = start_steps
        range_termination = get_range_termination(host_token, ring_tokens)
        formatter = format_murmur if is_murmur_ring(ring_tokens) else format_md5

        logging.info(
            "[{count}/{total}] repairing range ({token}, {termination}) in {steps} steps using {repair_concurrency} parallel processes for keyspace {keyspace}".format(
                count=token_num + 1,
                total=total_tokens,
                token=formatter(host_token), 
                termination=formatter(range_termination), 
                steps=steps,
                repair_concurrency=repair_concurrency,
                keyspace=keyspace))

        command_results = []

        def collect_results():
            pending = command_results[:]
            while pending:
                pending2 = pending[:]
                pending = []
                logging.debug("Collecting results from %s repairs" % len(pending2))
                for i, command_result in enumerate(pending2):
                    result, _, cmd, stdout, stderr = command_result.result()

                    if not result:
                        logging.error("FAILED: {0}".format(command_result.cmd))
                        logging.error(stderr)
                        pending.append(command_result.retry())

        processes = 0
        for start, end in get_sub_range_generator(host_token, range_termination, steps):
            start = formatter(start)
            end = formatter(end)

            logging.debug(
                "step {steps:04d} repairing range ({start}, {end}) for keyspace {keyspace}".format(
                    steps=steps,
                    start=start,
                    end=end,
                    keyspace=keyspace))

            command_results.append(repair_range(
                host=host,
                keyspace=keyspace,
                columnfamily=columnfamily,
                start=start,
                end=end,
                all_dcs=all_dcs))

            processes += 1

            if processes == repair_concurrency:
                collect_results()
                command_results = []
                processes = 0

            steps -= 1

        collect_results()

    return True

def main():
    """Validate arguments and initiate repair
    """
    parser = OptionParser()
    parser.add_option("-k", "--keyspace", dest="keyspace",
                      help="Keyspace to repair", metavar="KEYSPACE")

    parser.add_option("-c", "--columnfamily", dest="cf", default=None,
                      help="ColumnFamily to repair", metavar="COLUMNFAMILY")

    parser.add_option("-H", "--host", dest="host",
                      help="Hostname to repair", metavar="HOST")

    parser.add_option("-s", "--steps", dest="steps", type="int", default=100,
                      help="Number of discrete ranges", metavar="STEPS")

    parser.add_option("--nodetool", dest="nodetool", default=None,
                      help="Nodetool bin location", metavar="NODETOOL")

    parser.add_option("--repair-concurrency", dest="repair_concurrency", type="int", default=10,
                      help="How many repairs can run in parallel. Default: 10", metavar="REPAIRCONCURRENCY")

    parser.add_option("--all-dcs", dest="all_dcs", action="store_true", default=False,
                      help="Do not limit repair for local DC")

    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", default=False,
                      help="Verbose mode")

    (options, args) = parser.parse_args()

    if not options.keyspace:
        parser.print_help()
        sys.exit(1)

    global NODETOOL
    NODETOOL = options.nodetool

    setup_logging(loglevel='DEBUG' if options.verbose else None)
    repair_status = repair(
        keyspace=options.keyspace,
        columnfamily=options.cf,
        host=options.host,
        start_steps=options.steps,
        repair_concurrency=options.repair_concurrency,
        all_dcs=options.all_dcs)

    if repair_status:
        sys.exit(0)

    sys.exit(2)

if __name__ == "__main__":
    main()
