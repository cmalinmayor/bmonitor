import logging
import subprocess
from typing import Dict, Tuple, List
import re

logger = logging.getLogger(__name__)
# header strings
NJOBS = 'NJOBS'
PEND = 'PEND'
RUN = 'RUN'
DONE = 'DONE'
EXIT = 'EXIT'
STAT = 'STAT'

bsub_stdout_regex = re.compile("Job <(\d+)> is submitted*")

# TODO: Add bresubmit function


def get_last_jobid(array: bool = False) -> str:
    ''' Returns the jobid of the last submitted job (or array job)'''
    cmd = ['bjobs', '-a']
    if array:
        cmd = ['bjobs', '-A']
    all_jobs = subprocess.check_output(cmd, encoding='UTF-8')
    last_job = all_jobs.strip().split('\n')[-1]
    logger.debug("Last job: %s" % last_job)
    jobid = last_job.split()[0]
    return jobid


def is_ended(jobid: str, array: bool = False) -> bool:
    ''' Returns true if the job with job ID 'jobid' is finished
    running, whether sucessfully or not'''
    return is_done(jobid, array=array) or is_exit(jobid, array=array)


def _get_bjob_output(jobid: str, array: bool = False) -> List[List[str]]:
    ''' Helper function to get and parse the output of bjobs <jobid>.
    The output is split into lines (header and job info) and then tokens'''
    cmd = ['bjobs', jobid]
    if array:
        cmd.insert(1, '-A')
    bjob_output = subprocess.check_output(cmd, encoding='UTF-8')
    if 'Illegal job ID' in bjob_output:
        raise ValueError(bjob_output)
    bjob_output = bjob_output.strip().split('\n')
    assert len(bjob_output) == 2
    logger.debug("Bjob output:\n%s\n%s"
                 % (str(bjob_output[0]), str(bjob_output[1])))
    for i, line in enumerate(bjob_output):
        bjob_output[i] = line.strip().split()
    return bjob_output


def is_done(jobid: str, array: bool = False) -> bool:
    ''' Returns true if the job finished successfully. For an array,
    all jobs in the array must achieve status 'DONE' for this to be true'''
    # for arrays, only true if all jobs have status DONE
    bjob_output = _get_bjob_output(jobid, array=array)
    header, job_info = bjob_output
    if not array:
        return DONE in job_info
    else:
        num_jobs = int(job_info[header.index(NJOBS)])
        done = int(job_info[header.index(DONE)])
        return num_jobs == done


def is_exit(jobid: str, array: bool = False) -> bool:
    ''' Returns true if the job finished unsuccessfully. For an array,
    only one job in the array must achieve status 'EXIT' for this to be true,
    but all must have finished (have status 'DONE' or 'EXIT')'''
    # for arrays, true if all jobs are completed and at least
    # one job has status EXIT
    bjob_output = _get_bjob_output(jobid, array=array)
    header, job_info = bjob_output
    if not array:
        return EXIT in job_info
    else:
        num_jobs = int(job_info[header.index(NJOBS)])
        done = int(job_info[header.index(DONE)])
        exit = int(job_info[header.index(EXIT)])
        return num_jobs == done + exit and exit > 0


def get_array_length(jobid: str) -> int:
    return get_array_summary(jobid)[0]


def get_array_summary(jobid: str) -> Tuple[int, int, int, int, int]:
    ''' Returns a tuple summary of an array job with elements:
        (num_jobs, pending, running, done, exit)'''
    bjob_output = _get_bjob_output(jobid, array=True)
    header, job_info = bjob_output
    num_jobs = int(job_info[header.index(NJOBS)])
    pending = int(job_info[header.index(PEND)])
    running = int(job_info[header.index(RUN)])
    done = int(job_info[header.index(DONE)])
    exit = int(job_info[header.index(EXIT)])
    summary = (num_jobs, pending, running, done, exit)
    logger.debug(
        "Job array %s has %d jobs, %d pending, %d running, %d done, %d exited"
        % ((jobid,) + summary))
    return summary


def get_array_jobs_status(jobid: str) -> Dict[str, List[int]]:
    ''' Counts the jobs in an array with each status.
        Returns a dictionary from status string to job index '''
    array_length = get_array_length(jobid)
    status = {DONE: [], EXIT: [], PEND: [], RUN: []}
    for i in range(1, array_length + 1):
        sub_jobid = jobid + "[%d]" % i
        bjob_output = _get_bjob_output(sub_jobid, array=False)
        header, job_info = bjob_output
        stat = job_info[header.index(STAT)]
        status[stat].append(i)
    return status


def wait_for_job_end(jobid, timeout=None):
    ''' Calls bwait to return when job ends. Timeout (in seconds)
    is passed to subprocess.run, which throws a subprocess.TimeoutExpired
    exception if the bwait subprocess fails to return before the timeout.'''
    bwait = "bwait -w 'ended(%s)'" % jobid
    output = subprocess.check_call(bwait, shell=True, timeout=timeout)
    return output


def retry_failed(jobid, array=True):
    ''' Calls brequeue on exited jobs THIS DOESNT WORK'''
    array_length = get_array_length(jobid)
    retry_exited_cmd = 'brequeue -J "retry_%s[1-%d]" -e %s'\
                       % (jobid, array_length, jobid)
    output = subprocess.run(
            retry_exited_cmd,
            shell=True,
            stdout=subprocess.PIPE,
            encoding='UTF-8')
    match = bsub_stdout_regex.match(output.stdout)
    if not match:
        logger.warn("Could not get jobid")
    jobid = match.group(1)
    return jobid
