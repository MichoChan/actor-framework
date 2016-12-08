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

#include "caf/policy/numa_aware_work_stealing.hpp"

namespace caf {
namespace policy {

numa_aware_work_stealing::~numa_aware_work_stealing() {
  // nop
}

std::ostream& operator<<(std::ostream& s, const numa_aware_work_stealing::hwloc_bitmap_wrapper& w) {
  char* tmp = nullptr;
  hwloc_bitmap_asprintf(&tmp, w.get());
  s << std::string(tmp);
  free(tmp);
  return s;
}

numa_aware_work_stealing::pu_set_t
numa_aware_work_stealing::get_pu_set(const topo_ptr& topo,
                                     const node_set_t& node_set) const {
  pu_set_t pu_set = hwloc_bitmap_make_wrapper();
  hwloc_cpuset_from_nodeset(topo.get(), pu_set.get(), node_set.get());
  return pu_set;
}

numa_aware_work_stealing::node_set_t
numa_aware_work_stealing::get_node_set(const topo_ptr& topo,
                                       const pu_set_t& pu_set) const {
  node_set_t node_set = hwloc_bitmap_make_wrapper();
  hwloc_cpuset_to_nodeset(topo.get(), pu_set.get(), node_set.get());
  return node_set;
}

numa_aware_work_stealing::pu_matrix_t numa_aware_work_stealing::get_pu_matrix(
  const topo_ptr& topo, const pu_set_t& current_pu_id_set) const {
  pu_matrix_t result_matrix;
  // Distance matrix of NUMA nodes.
  // It is possible to request the distance matrix on PU level, 
  // which would be a better match for our usescase
  // but on all tested hardware it has returned a nullptr, maybe future work?
  auto distance_matrix =
    hwloc_get_whole_distance_matrix_by_type(topo.get(), HWLOC_OBJ_NUMANODE);
  CALL_CAF_CRITICAL(!distance_matrix || !distance_matrix->latency,
                    "NUMA distance matrix not available");
  auto current_node_set = get_node_set(topo, current_pu_id_set);
  CALL_CAF_CRITICAL(hwloc_bitmap_iszero(current_node_set.get()),
                    "Current NUMA node_set is unknown");
  node_id_t current_node_id = hwloc_bitmap_first(current_node_set.get());
  node_id_t num_of_dist_objs = distance_matrix->nbobjs;
  // relvant line for the current NUMA node in distance matrix
  float* dist_pointer =
    &distance_matrix->latency[num_of_dist_objs * current_node_id];
  std::map<float, pu_set_t> dist_map;
  // iterate over all NUMA nodes and classify them in distance levels regarding
  // to the current NUMA node
  for (node_id_t x = 0; x < num_of_dist_objs; ++x) {
    node_set_t tmp_node_set = hwloc_bitmap_make_wrapper();
    hwloc_bitmap_set(tmp_node_set.get(), x);
    pu_set_t tmp_pu_set = get_pu_set(topo, tmp_node_set);
    // you cannot steal from yourself
    if (x == current_node_id) {
      hwloc_bitmap_andnot(tmp_pu_set.get(), tmp_pu_set.get(),
                          current_pu_id_set.get());
    }
    auto dist_it = dist_map.find(dist_pointer[x]);
    if (dist_it == dist_map.end())
      // create a new distane level
      dist_map.insert(std::make_pair(dist_pointer[x], std::move(tmp_pu_set)));
    else
      // add PUs to an available distance level
      hwloc_bitmap_or(dist_it->second.get(), dist_it->second.get(),
                      tmp_pu_set.get());
  }
  // return PU matrix sorted by its distance
  result_matrix.reserve(dist_map.size());
  for (auto& it : dist_map) {
    result_matrix.emplace_back(std::move(it.second));
  }
  return result_matrix;

}



} // namespace policy
} // namespace caf
