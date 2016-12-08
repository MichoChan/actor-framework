/******************************************************************************
 *                       ____    _    _____                                   *
 *                      / ___|  / \  |  ___|    C++                           *
 *                     | |     / _ \ | |_       Actor                         *
 *                     | |___ / ___ \|  _|      Framework                     *
 *                      \____/_/   \_|_|                                      *
 *                                                                            *
 * Copyright (C) 2011 - 2016                                                  *
 * Dominik Charousset <dominik.charousset (at) haw-hamburg.de>                *
 *                                                                            *
 * Distributed under the terms and conditions of the BSD 3-Clause License or  *
 * (at your option) under the terms and conditions of the Boost Software      *
 * License 1.0. See accompanying files LICENSE and LICENSE_ALTERNATIVE.       *
 *                                                                            *
 * If you did not receive a copy of the license files, see                    *
 * http://opensource.org/licenses/BSD-3-Clause and                            *
 * http://www.boost.org/LICENSE_1_0.txt.                                      *
 ******************************************************************************/

#ifndef CAF_POLICY_NUMA_AWARE_WORK_STEALING_HPP
#define CAF_POLICY_NUMA_AWARE_WORK_STEALING_HPP

#include <deque>
#include <chrono>
#include <thread>
#include <random>
#include <cstddef>

#include <hwloc.h>

#include "caf/policy/work_stealing.hpp"

namespace caf {
namespace policy {

#define CALL_CAF_CRITICAL(predicate, msg)  \
  if (predicate)                           \
    CAF_CRITICAL(msg)

/// Implements scheduling of actors via a numa aware work stealing.
/// @extends scheduler_policy
class numa_aware_work_stealing : public work_stealing {
public:
  ~numa_aware_work_stealing();

  struct hwloc_bitmap_topo_free {
    void operator()(hwloc_topology_t p) {
      hwloc_topology_destroy(p);
    }
  };

  using topo_ptr = std::unique_ptr<hwloc_topology, hwloc_bitmap_topo_free>;

  template <class Worker>
  struct worker_deleter {
    worker_deleter(topo_ptr& t) 
      : topo(t)
    { };
    void operator()(void * p) {
      hwloc_free(topo.get(), p, sizeof(Worker));
    }
    topo_ptr& topo;
  };
  
  struct hwloc_bitmap_free_wrapper {
    void operator()(hwloc_bitmap_t p) {
      hwloc_bitmap_free(p);
    }
  };

  using hwloc_bitmap_wrapper =
    std::unique_ptr<hwloc_bitmap_s, hwloc_bitmap_free_wrapper>;

  hwloc_bitmap_wrapper hwloc_bitmap_make_wrapper() const {
    return hwloc_bitmap_wrapper(hwloc_bitmap_alloc());
  }

  using pu_id_t = int;
  using node_id_t = int;
  using pu_set_t = hwloc_bitmap_wrapper;
  using node_set_t = hwloc_bitmap_wrapper;
  using pu_matrix_t = std::vector<pu_set_t>;

  template <class Worker>
  struct coordinator_data {
    inline explicit coordinator_data(scheduler::abstract_coordinator*) {
      bool res;
      hwloc_topology_t raw_topo;
      res = hwloc_topology_init(&raw_topo);
      CALL_CAF_CRITICAL(res == -1, "hwloc_topology_init() failed");
      topo.reset(raw_topo);
      res = hwloc_topology_load(topo.get());
      CALL_CAF_CRITICAL(res == -1, "hwloc_topology_load() failed");
      pu_depth = hwloc_get_type_depth(topo.get(), HWLOC_OBJ_PU);
      CALL_CAF_CRITICAL(pu_depth == HWLOC_TYPE_DEPTH_UNKNOWN,
                        "Type HWLOC_OBJ_PU not found");
      num_pus = hwloc_get_nbobjs_by_depth(topo.get(), pu_depth);
      CALL_CAF_CRITICAL(num_pus == 0, "no PUs found");
      next_worker = 0;
    }
    topo_ptr topo;
    int pu_depth;
    size_t num_pus;
    std::vector<std::unique_ptr<Worker, worker_deleter<Worker>>> workers;
    // used by central enqueue to balance new jobs between workers with round
    // robin strategy
    std::atomic<size_t> next_worker; 
  };

  struct worker_data {
    inline explicit worker_data(scheduler::abstract_coordinator* p)
        :  strategies(get_poll_strategies(p)) {

    }
    // This queue is exposed to other workers that may attempt to steal jobs
    // from it and the central scheduling unit can push new jobs to the queue.
    queue_type queue;
    pu_matrix_t pu_dist_matrix;
    std::vector<poll_strategy> strategies;
  };

  /// Creates a new worker.
  template <class Coordinator, class Worker>
  std::unique_ptr<Worker, worker_deleter<Worker>>
  create_worker(Coordinator* self, size_t worker_id, size_t throughput) {
    auto& cdata = d(self);
    auto& topo = cdata.topo;
    CALL_CAF_CRITICAL(cdata.num_pus - 1 < worker_id,
                      "worker_id higher than the number of PUs");
    auto pu_set = hwloc_bitmap_make_wrapper();
    hwloc_bitmap_set(pu_set.get(), worker_id);
    auto node_set = get_node_set(topo, pu_set);
    auto ptr =
      hwloc_alloc_membind_nodeset(topo.get(), sizeof(Worker), node_set.get(),
                                  HWLOC_MEMBIND_BIND, HWLOC_MEMBIND_THREAD);
    std::unique_ptr<Worker, worker_deleter<Worker>> res(
      new (ptr) Worker(worker_id, self, throughput),
      worker_deleter<Worker>(topo));
    return res;
  }

  /// Initalize worker thread.
  template <class Worker>
  void init_worker_thread(Worker* self) {
    auto& wdata = d(self);
    auto& cdata = d(self->parent());
    auto pu_set = hwloc_bitmap_make_wrapper();
    hwloc_bitmap_set(pu_set.get(), self->id());
    auto res = hwloc_set_cpubind(cdata.topo.get(), pu_set.get(),
                          HWLOC_CPUBIND_THREAD | HWLOC_CPUBIND_NOMEMBIND);
    CALL_CAF_CRITICAL(res == -1, "hwloc_set_cpubind() failed");
    wdata.pu_dist_matrix = get_pu_matrix(cdata.topo, pu_set);
  }

  template <class Worker>
  resumable* try_steal(Worker* self, size_t& current_steal_lvl,
                       pu_id_t& last_pu_id) {
    auto p = self->parent();
    pu_id_t num_workers = p->num_workers();
    if (num_workers < 2) {
      // you can't steal from yourself, can you?
      return nullptr;
    }
    auto& dmatrix = d(self).pu_dist_matrix;
    // iterate over each distance level
    while(current_steal_lvl < dmatrix.size()) {
      auto& pu_set = dmatrix[current_steal_lvl];
      // iterate over each pu_id in current distance level
      for (last_pu_id = hwloc_bitmap_next(pu_set.get(), last_pu_id);
           last_pu_id != -1;
           last_pu_id = hwloc_bitmap_next(pu_set.get(), last_pu_id)) {
        if (last_pu_id < num_workers)
          return d(p->worker_by_id(last_pu_id)).queue.take_tail();
      }
      ++current_steal_lvl;
      last_pu_id = -1;
    }
    // tried to steal from all workes with no success
    // but never resign and start again from the beginning
    current_steal_lvl = 0;
    last_pu_id = -1;
    return nullptr;
  }

  template <class Worker>
  resumable* dequeue(Worker* self) {
    // we wait for new jobs by polling our external queue: first, we
    // assume an active work load on the machine and perform aggresive
    // polling, then we relax our polling a bit and wait 50 us between
    // dequeue attempts, finally we assume pretty much nothing is going
    // on and poll every 10 ms; this strategy strives to minimize the
    // downside of "busy waiting", which still performs much better than a
    // "signalizing" implementation based on mutexes and conition variables
    size_t current_steal_lvl = 0;
    pu_id_t last_pu_id = -1;
    auto& strategies = d(self).strategies;
    resumable* job = nullptr;
    for (auto& strat : strategies) {
      for (size_t i = 0; i < strat.attempts; i += strat.step_size) {
        job = d(self).queue.take_head();
        if (job)
          return job;
        // try to steal every X poll attempts
        if ((i % strat.steal_interval) == 0) {
          job = try_steal(self, current_steal_lvl, last_pu_id);
          if (job)
            return job;
        }
        if (strat.sleep_duration.count() > 0)
          std::this_thread::sleep_for(strat.sleep_duration);
      }
    }
    // unreachable, because the last strategy loops
    // until a job has been dequeued
    return nullptr;
  }
private:
  pu_set_t get_pu_set(const topo_ptr& topo, const node_set_t& node_set) const;
  node_set_t get_node_set(const topo_ptr& topo, const pu_set_t& pu_set) const;
  pu_matrix_t get_pu_matrix(const topo_ptr& topo,
                            const pu_set_t& current_pu_id_set) const;

  // -- debug stuff --
  friend std::ostream& operator <<(std::ostream& s, const hwloc_bitmap_wrapper& w);
};


} // namespace policy
} // namespace caf

#endif // CAF_POLICY_NUMA_AWARE_WORK_STEALING_HPP
