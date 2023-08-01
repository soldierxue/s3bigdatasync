# -*- coding:utf-8 -*-
import os
import sys
import asyncio
import queue
import multiprocessing as mp
import conf
import time
import signal
import logging
import psutil

# setup logging
logger = logging.getLogger(__name__)

def killall_children():
    current_process = psutil.Process()
    children = current_process.children(recursive=True)
    for child in children:
        os.kill(child.pid, signal.SIGTERM)

def app_sigterm_handler(signum, frame):
    if signum == signal.SIGTERM:
        killall_children()
        sys.exit(0)

class MpReactorManager:

    def __init__(self, main_ctx, main_task_init, main_task_gen, main_result_collector, task_init_func, task_func, scale_condition_func):
        mp.set_start_method('fork')

        self.os_cpus = os.cpu_count()
        self.init_workers = conf.INIT_WORKERS
        self.max_workers = conf.MAX_WORKERS
        self.res_queue = mp.Queue()
        self.task_queue = mp.Queue()
        self.worker_count = mp.Value('i', 0)
        self.completed = 0

        self.main_ctx = main_ctx
        self.main_task_init = main_task_init
        self.main_task_gen = main_task_gen
        self.main_result_collector = main_result_collector
        self.task_init_func = task_init_func
        self.task_func = task_func
        self.scale_condition_func = scale_condition_func

        logger.info("MpReactorManager init")
        logger.info(" - init procs {}, OS CPUs {}".format(self.init_workers, self.os_cpus))
        logger.info(" - updte interval {}s".format(conf.UPDATE_INTERVAL))
        logger.info(" - task queue size {}, result queue size {}".format(self.task_queue.qsize(), self.res_queue.qsize()))

    def child_count(self):
        return len(mp.active_children())

    def tick(self):

        time.sleep(conf.UPDATE_INTERVAL)

        condition = self.scale_condition_func()
        taskq_size = self.task_queue.qsize()
        if condition <= -1 or taskq_size == 0:
            if self.worker_count.value > self.init_workers:
                # reduce process
                self.worker_count.value = self.worker_count.value - 1
                logger.info("reduce worker to {}".format(self.worker_count.value))
        elif condition >= 1 and taskq_size > 0:
            # worker count start from 1
            if self.worker_count.value == self.max_workers:
                logger.info("reached MAX worker {}".format(self.max_workers))
            else:
                # create new proc
                worker_id = self.worker_count.value + 1
                p = mp.Process(target=self.proc_reactor_func, args=(self.task_init_func, self.task_func, self.res_queue, self.task_queue, self.worker_count, worker_id, conf.DRY_RUN))
                p.start()
                self.worker_count.value += 1

        # must call child count here or python process will  [python] <defunct> in quit
        self.child_count()

        #logger.debug("Manager - current mp children {}, worker count {}, taskq size {}"
        #    .format(self.child_count(), self.worker_count.value, taskq_size))

    async def reactor(self, task_init_func, task_func, res_queue, task_queue, worker_count, worker_id, dry_run):

        ctx = task_init_func()

        # infinite loop
        while True:
            try:
                task_msg = task_queue.get_nowait()

            except queue.Empty:
                if worker_count.value < worker_id:
                    task_queue.close()
                    return

                logger.debug("sleep in worker id {}/{} pid {}".format(worker_id, worker_count.value, os.getpid()))
                await asyncio.sleep(conf.UPDATE_INTERVAL)
                continue

            start = time.time_ns()

            # do task here
            logger.debug("do task in proc id {} pid {}".format(worker_id, os.getpid()))
            try:
                result = await task_func(ctx, task_msg, worker_id)
            except Exception as e:
                import traceback
                logger.warning("unhandled exception in worker id: " + str(worker_id))
                logger.warning('{exception}'.format(exception=traceback.format_exc()))
                task_msg.err_msg = str(e)
                task_msg.ts_end = int(time.time())
                result = {'result': 'exception', 'msg': task_msg}

            cost = time.time_ns() - start
            res_queue.put((result, cost))

            if worker_count.value < worker_id:
                task_queue.close()
                return

    def proc_reactor_func(self, task_init_func, task_func, res_queue, task_queue, worker_count, worker_id, dry_run):
        asyncio.run(self.reactor(task_init_func, task_func, res_queue, task_queue, worker_count, worker_id, dry_run))
        sys.exit(0)

    def proc_main_func(self, res_queue, task_queue, worker_count, worker_id, dry_run):

        assert worker_id == 0

        main_ctx = self.main_task_init(self.main_ctx)

        # start main loop
        start = time.time()
        count = 1 
        while True:

            # if we reach high water mark of task queue, sleep for a while and retry
            task_queue_size = task_queue.qsize()
            if task_queue_size >= conf.TASK_QUEUE_HIGH_WATERMARK:
                logger.info("task queue high water mark reached {}/{}"
                    .format(task_queue_size, conf.TASK_QUEUE_HIGH_WATERMARK)
                )
                time.sleep(conf.UPDATE_INTERVAL)
                continue

            task_msgs = self.main_task_gen(main_ctx)
            for task_msg in task_msgs:
                task_queue.put(task_msg)

            results = []
            total_cost = 0

            # collect all results in possible
            while True:
                try:
                    (result, cost) = res_queue.get_nowait()
                    total_cost = total_cost + cost
                    results.append(result)
                except queue.Empty:
                    # get chance to let main loop have a rest
                    time.sleep(conf.UPDATE_INTERVAL)
                    break

            if len(results) > 0:
                self.main_result_collector(main_ctx, results)

    def run(self):

        # register termination handler
        signal.signal(signal.SIGTERM, app_sigterm_handler)

        # init main proc
        worker_id = 0
        main = mp.Process(target=self.proc_main_func, args=(self.res_queue, self.task_queue, self.worker_count, worker_id, conf.DRY_RUN))
        main.start()

        # init base procs
        for i in range(self.init_workers):
            worker_id = self.worker_count.value + 1
            p = mp.Process(target=self.proc_reactor_func, args=(self.task_init_func, self.task_func, self.res_queue, self.task_queue, self.worker_count, worker_id, conf.DRY_RUN))
            p.start()
            self.worker_count.value += 1

        while True:
            self.tick()

        main.join()
