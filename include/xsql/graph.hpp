// Copyright (c) 2024-2026 Elias Bachaalany
// SPDX-License-Identifier: LicenseRef-Human-Origin-Source-1.0
//
// This file is licensed under the Human-Origin Source License v1.0.
// See LICENSE.

/**
 * graph.hpp - a general-purpose directed-graph algorithm utility.
 *
 * This is a STOCK graph library, not a reverse-engineering facility: it knows
 * nothing about addresses, basic blocks, control flow, or any application
 * concept. A graph is a node count plus a directed edge list over opaque integer
 * node ids in `[0, node_count)`. Any caller that can map its own objects onto
 * integer ids can use it.
 *
 * Provided, all on opaque node ids:
 *   - immediate_dominators / dominator_sets (relative to an entry)
 *   - immediate_post_dominators (relative to the graph's sinks, via a virtual exit)
 *   - natural_loops (back-edge + dominator based)
 *   - strongly_connected_components (Tarjan)
 *   - topological_order (Kahn; std::nullopt when the graph has a cycle)
 *
 * Header-only, no dependencies beyond the standard library, to match the rest of
 * libxsql.
 */

#pragma once

#include <algorithm>
#include <cstddef>
#include <functional>
#include <optional>
#include <queue>
#include <stdexcept>
#include <utility>
#include <vector>

namespace xsql {
namespace graph {

// Sentinel for "no node" (unreachable node's dominator, a node that post-dominates
// straight to the virtual exit, etc.).
inline constexpr std::size_t kNoNode = static_cast<std::size_t>(-1);

// A directed graph over node ids [0, node_count). Parallel edges and self-loops are
// permitted; algorithms below tolerate both.
class DirectedGraph {
public:
    explicit DirectedGraph(std::size_t node_count)
        : succ_(node_count), pred_(node_count) {}

    std::size_t node_count() const { return succ_.size(); }

    // Add a directed edge from -> to. Both ids must be < node_count(); an
    // out-of-range id throws std::out_of_range. This is the single validation
    // point — every algorithm below trusts stored edges to be in range.
    void add_edge(std::size_t from, std::size_t to) {
        if (from >= succ_.size() || to >= succ_.size()) {
            throw std::out_of_range(
                "DirectedGraph::add_edge: node id out of range");
        }
        succ_[from].push_back(to);
        pred_[to].push_back(from);
    }

    const std::vector<std::size_t>& successors(std::size_t n) const { return succ_[n]; }
    const std::vector<std::size_t>& predecessors(std::size_t n) const { return pred_[n]; }

private:
    std::vector<std::vector<std::size_t>> succ_;
    std::vector<std::vector<std::size_t>> pred_;
};

namespace detail {

// Iterative DFS from `entry` producing a postorder listing (children finish before
// their parent; `entry` is last). `on_succ(n) -> range` selects the traversal
// direction so the same routine serves forward and reverse graphs.
template <typename SuccFn>
inline std::vector<std::size_t> postorder_from(std::size_t node_count,
                                               std::size_t entry, SuccFn on_succ) {
    std::vector<std::size_t> order;
    if (entry >= node_count) return order;
    std::vector<char> visited(node_count, 0);
    // stack of (node, next-successor-index)
    std::vector<std::pair<std::size_t, std::size_t>> stack;
    visited[entry] = 1;
    stack.emplace_back(entry, 0);
    while (!stack.empty()) {
        auto& [n, i] = stack.back();
        const auto& succ = on_succ(n);
        if (i < succ.size()) {
            std::size_t m = succ[i++];
            if (!visited[m]) {
                visited[m] = 1;
                stack.emplace_back(m, 0);
            }
        } else {
            order.push_back(n);
            stack.pop_back();
        }
    }
    return order;
}

// Cooper-Harvey-Kennedy immediate dominators over a caller-supplied predecessor and
// successor accessor (so it works on a forward graph OR a virtual-exit-augmented
// reverse graph). idom[entry] = entry; unreachable nodes keep kNoNode.
template <typename SuccFn, typename PredFn>
inline std::vector<std::size_t> idom_generic(std::size_t node_count, std::size_t entry,
                                             SuccFn on_succ, PredFn on_pred) {
    std::vector<std::size_t> idom(node_count, kNoNode);
    if (entry >= node_count) return idom;

    std::vector<std::size_t> post = postorder_from(node_count, entry, on_succ);
    // postnum[n] = postorder index (entry has the largest); kNoNode if unreachable.
    std::vector<std::size_t> postnum(node_count, kNoNode);
    for (std::size_t i = 0; i < post.size(); ++i) postnum[post[i]] = i;

    // Reverse postorder (entry first) over the reachable set only.
    std::vector<std::size_t> rpo(post.rbegin(), post.rend());

    auto intersect = [&](std::size_t a, std::size_t b) -> std::size_t {
        while (a != b) {
            while (postnum[a] < postnum[b]) a = idom[a];
            while (postnum[b] < postnum[a]) b = idom[b];
        }
        return a;
    };

    idom[entry] = entry;
    bool changed = true;
    while (changed) {
        changed = false;
        for (std::size_t b : rpo) {
            if (b == entry) continue;
            std::size_t new_idom = kNoNode;
            for (std::size_t p : on_pred(b)) {
                if (postnum[p] == kNoNode) continue;             // unreachable pred
                if (idom[p] == kNoNode && p != entry) continue;  // not yet processed
                new_idom = (new_idom == kNoNode) ? p : intersect(p, new_idom);
            }
            if (new_idom != kNoNode && idom[b] != new_idom) {
                idom[b] = new_idom;
                changed = true;
            }
        }
    }
    return idom;
}

}  // namespace detail

// Immediate dominators relative to `entry`. idom[entry] == entry; a node
// unreachable from entry keeps kNoNode.
inline std::vector<std::size_t> immediate_dominators(const DirectedGraph& g,
                                                     std::size_t entry) {
    return detail::idom_generic(
        g.node_count(), entry,
        [&](std::size_t n) -> const std::vector<std::size_t>& { return g.successors(n); },
        [&](std::size_t n) -> const std::vector<std::size_t>& { return g.predecessors(n); });
}

// Full dominator sets: dominators[n] is the sorted set of all nodes that dominate n
// (including n itself). Unreachable nodes get an empty set.
//
// COST: the output itself is O(n^2) in the worst case (a linear chain of n nodes
// stores 1+2+...+n ids — 16k nodes is ~128M entries, gigabytes of memory). Use
// immediate_dominators for large graphs and walk the idom chain on demand; this
// helper is for small graphs where materializing every set is convenient.
inline std::vector<std::vector<std::size_t>> dominator_sets(const DirectedGraph& g,
                                                            std::size_t entry) {
    std::vector<std::size_t> idom = immediate_dominators(g, entry);
    std::vector<std::vector<std::size_t>> dom(g.node_count());
    for (std::size_t n = 0; n < g.node_count(); ++n) {
        if (idom[n] == kNoNode) continue;  // unreachable
        std::size_t cur = n;
        dom[n].push_back(cur);
        while (cur != entry) {
            cur = idom[cur];
            dom[n].push_back(cur);
        }
        std::sort(dom[n].begin(), dom[n].end());
    }
    return dom;
}

// Immediate post-dominators. Every node's ipdom is the first node on every path
// from it to a graph sink (a node with no successors). Computed as immediate
// dominators on the reversed graph rooted at a virtual exit that all sinks feed.
//
// SENTINEL: kNoNode deliberately conflates two distinct outcomes — "n's paths
// converge only at the virtual exit" (n is a sink, or its paths meet no earlier
// than exit) and "n cannot reach any sink at all" (e.g. n is stuck in an
// infinite loop). Callers that need the distinction can separate the cases
// themselves: g.successors(n).empty() identifies a sink directly, and a
// reachability walk to the sink set identifies the second case. Splitting the
// sentinel would change every consumer's public row shape, so it is documented
// rather than redesigned here.
inline std::vector<std::size_t> immediate_post_dominators(const DirectedGraph& g) {
    const std::size_t n = g.node_count();
    const std::size_t virt = n;  // virtual exit id

    // Reverse adjacency augmented with the virtual exit. rev_succ = original preds;
    // the virtual exit's successors are all sinks; each sink's predecessor is virt.
    std::vector<std::vector<std::size_t>> rev_succ(n + 1);
    std::vector<std::vector<std::size_t>> rev_pred(n + 1);
    auto add = [&](std::size_t from, std::size_t to) {
        rev_succ[from].push_back(to);
        rev_pred[to].push_back(from);
    };
    for (std::size_t u = 0; u < n; ++u) {
        for (std::size_t v : g.successors(u)) add(v, u);  // reverse each edge
        if (g.successors(u).empty()) add(virt, u);        // sink <- virtual exit
    }

    std::vector<std::size_t> ipdom = detail::idom_generic(
        n + 1, virt,
        [&](std::size_t x) -> const std::vector<std::size_t>& { return rev_succ[x]; },
        [&](std::size_t x) -> const std::vector<std::size_t>& { return rev_pred[x]; });

    // Trim the virtual exit; map ipdom==virt (or self at the virtual root) to kNoNode.
    std::vector<std::size_t> out(n, kNoNode);
    for (std::size_t x = 0; x < n; ++x) {
        std::size_t d = ipdom[x];
        out[x] = (d == kNoNode || d == virt) ? kNoNode : d;
    }
    return out;
}

// A natural loop: the header (loop entry that dominates the whole body), the latch
// (the back-edge tail), and the sorted set of body nodes (header + latch included).
struct NaturalLoop {
    std::size_t header = kNoNode;
    std::size_t latch = kNoNode;
    std::vector<std::size_t> body;
};

// Natural loops of the graph relative to `entry`. For each edge latch -> header
// where header dominates latch (a back edge), the loop body is header plus every
// node that reaches latch without passing through header.
inline std::vector<NaturalLoop> natural_loops(const DirectedGraph& g, std::size_t entry) {
    const std::size_t n = g.node_count();
    std::vector<std::size_t> idom = immediate_dominators(g, entry);

    auto dominates = [&](std::size_t a, std::size_t b) -> bool {
        if (idom[b] == kNoNode) return false;  // b unreachable
        std::size_t cur = b;
        while (true) {
            if (cur == a) return true;
            if (cur == entry) return a == entry;
            cur = idom[cur];
        }
    };

    // Collect back edges first and dedup (latch, header) pairs: parallel edges
    // are permitted by the graph contract, and a duplicated back edge must
    // yield ONE loop, not N byte-identical records.
    std::vector<std::pair<std::size_t, std::size_t>> back_edges;  // (latch, header)
    for (std::size_t latch = 0; latch < n; ++latch) {
        if (idom[latch] == kNoNode) continue;  // unreachable latch
        for (std::size_t header : g.successors(latch)) {
            if (!dominates(header, latch)) continue;  // not a back edge
            back_edges.emplace_back(latch, header);
        }
    }
    std::sort(back_edges.begin(), back_edges.end());
    back_edges.erase(std::unique(back_edges.begin(), back_edges.end()),
                     back_edges.end());

    // One generation-stamped membership buffer shared across loops, and body
    // nodes collected as they are marked: a fresh O(n) bitmap plus an [0,n)
    // rescan per back edge made loop discovery quadratic on large graphs.
    std::vector<std::size_t> stamp(n, 0);
    std::size_t generation = 0;
    std::vector<std::size_t> stack;
    std::vector<NaturalLoop> loops;
    loops.reserve(back_edges.size());
    for (const auto& edge : back_edges) {
        const std::size_t latch = edge.first;
        const std::size_t header = edge.second;
        ++generation;
        NaturalLoop loop;
        loop.header = header;
        loop.latch = latch;
        auto mark = [&](std::size_t x) {
            if (stamp[x] == generation) return false;
            stamp[x] = generation;
            loop.body.push_back(x);
            return true;
        };
        mark(header);                          // header bounds the backward walk
        if (mark(latch)) stack.push_back(latch);
        while (!stack.empty()) {
            std::size_t m = stack.back();
            stack.pop_back();
            for (std::size_t p : g.predecessors(m)) {
                if (idom[p] != kNoNode && mark(p)) {
                    stack.push_back(p);
                }
            }
        }
        std::sort(loop.body.begin(), loop.body.end());
        loops.push_back(std::move(loop));
    }
    return loops;
}

// Strongly connected components (Tarjan, iterative). Returns comp[n] = a component
// id in [0, component_count); nodes in the same SCC share an id. Component ids are
// assigned in reverse topological order of the condensation (Tarjan's natural order).
inline std::vector<std::size_t> strongly_connected_components(const DirectedGraph& g) {
    const std::size_t n = g.node_count();
    std::vector<std::size_t> comp(n, kNoNode);
    std::vector<std::size_t> index(n, kNoNode), low(n, 0);
    std::vector<char> on_stack(n, 0);
    std::vector<std::size_t> scc_stack;
    std::size_t next_index = 0, next_comp = 0;

    // Explicit work stack of (node, next-successor-index) for iterative Tarjan.
    std::vector<std::pair<std::size_t, std::size_t>> work;
    for (std::size_t root = 0; root < n; ++root) {
        if (index[root] != kNoNode) continue;
        work.emplace_back(root, 0);
        while (!work.empty()) {
            auto& [v, i] = work.back();
            if (i == 0) {
                index[v] = low[v] = next_index++;
                scc_stack.push_back(v);
                on_stack[v] = 1;
            }
            const auto& succ = g.successors(v);
            if (i < succ.size()) {
                std::size_t w = succ[i++];
                if (index[w] == kNoNode) {
                    work.emplace_back(w, 0);  // recurse
                } else if (on_stack[w]) {
                    low[v] = (std::min)(low[v], index[w]);
                }
            } else {
                const std::size_t completed = v;
                if (low[completed] == index[completed]) {
                    while (true) {
                        std::size_t w = scc_stack.back();
                        scc_stack.pop_back();
                        on_stack[w] = 0;
                        comp[w] = next_comp;
                        if (w == completed) break;
                    }
                    ++next_comp;
                }
                work.pop_back();
                if (!work.empty()) {
                    std::size_t parent = work.back().first;
                    low[parent] = (std::min)(low[parent], low[completed]);
                }
            }
        }
    }
    return comp;
}

// Topological order (Kahn). Returns std::nullopt if the graph has a directed cycle;
// otherwise a full ordering of all nodes with every edge pointing forward.
inline std::optional<std::vector<std::size_t>> topological_order(const DirectedGraph& g) {
    const std::size_t n = g.node_count();
    std::vector<std::size_t> indeg(n, 0);
    for (std::size_t u = 0; u < n; ++u)
        for (std::size_t v : g.successors(u))
            ++indeg[v];

    // Smallest-id-first for a deterministic result. A min-heap keeps that
    // ordering in O(E log n); the previous sorted-vector middle-insert was
    // O(n) per newly-ready node — quadratic on wide graphs.
    std::priority_queue<std::size_t, std::vector<std::size_t>,
                        std::greater<std::size_t>>
        ready;
    for (std::size_t u = 0; u < n; ++u)
        if (indeg[u] == 0) ready.push(u);

    std::vector<std::size_t> order;
    order.reserve(n);
    while (!ready.empty()) {
        std::size_t u = ready.top();
        ready.pop();
        order.push_back(u);
        for (std::size_t v : g.successors(u)) {
            if (--indeg[v] == 0) {
                ready.push(v);
            }
        }
    }
    if (order.size() != n) return std::nullopt;  // cycle
    return order;
}

}  // namespace graph
}  // namespace xsql
