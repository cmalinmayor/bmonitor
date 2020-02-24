import unittest
import subprocess
import bmonitor as bm
import time

import logging
logging.basicConfig(level=logging.WARN)


class TestBmonitor(unittest.TestCase):
    # tests must be run with access to a bsub cluster
    def test_regular_job(self):
        start_job_cmd = ['bsub',
                         '-n 1',
                         '-J test_bmonitor',
                         '-o test_bmonitor.%J.log',
                         '-W 1',
                         'sleep 10']
        subprocess.run(' '.join(start_job_cmd), shell=True)
        jobid = bm.get_last_jobid()
        print("Job id: %s" % jobid)
        self.assertFalse(bm.is_ended(jobid))
        self.assertFalse(bm.is_done(jobid))
        self.assertFalse(bm.is_exit(jobid))

        time.sleep(20)
        self.assertTrue(bm.is_ended(jobid))
        self.assertTrue(bm.is_done(jobid))
        self.assertFalse(bm.is_exit(jobid))

    def test_array_job(self):
        start_job_cmd = ['bsub',
                         '-n', '1',
                         '-J', 'test_bmonitor[1-5]',
                         '-o', 'test_bmonitor.%J.%I.log',
                         '-W', '1',
                         '"sleep \\$LSB_JOBINDEX"']
        subprocess.run(' '.join(start_job_cmd), shell=True)
        jobid = bm.get_last_jobid()
        print("Job id: %s" % jobid)
        self.assertFalse(bm.is_ended(jobid, array=True))
        self.assertFalse(bm.is_done(jobid, array=True))
        self.assertFalse(bm.is_exit(jobid, array=True))
        self.assertEqual(bm.get_array_length(jobid), 5)

        time.sleep(1)
        self.assertFalse(bm.is_ended(jobid, array=True))

        time.sleep(15)
        self.assertTrue(bm.is_ended(jobid, array=True))
        self.assertTrue(bm.is_done(jobid, array=True))
        self.assertFalse(bm.is_exit(jobid, array=True))
        self.assertEqual(bm.get_array_summary(jobid),
                         (5, 0, 0, 5, 0))
        jobs_status = bm.get_array_jobs_status(jobid)
        self.assertEqual(len(jobs_status['DONE']), 5)
