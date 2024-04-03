#pragma once

#include "kmer_t.hpp"
#include <upcxx/upcxx.hpp>

struct HashMap {
    std::vector<kmer_pair> data;
    std::vector<int> used;

    size_t my_size;

    size_t size() const noexcept;

    HashMap(size_t size);

    // Most important functions: insert and retrieve
    // k-mers from the hash table.
    bool insert(const kmer_pair& kmer);
    bool find(const pkmer_t& key_kmer, kmer_pair& val_kmer);

    // Helper functions

    int determine_owner(uint64_t slot);

    // Write and read to a logical data slot in the table.
    void write_slot(uint64_t slot, const kmer_pair& kmer);
    kmer_pair read_slot(uint64_t slot);

    // Request a slot or check if it's already used.
    bool request_slot(uint64_t slot);
    bool slot_used(uint64_t slot);
};

int HashMap::determine_owner(uint64_t slot) {
    return slot % upcxx::rank_n();
}

HashMap::HashMap(size_t size) {
    my_size = size;
    data.resize(size);
    used.resize(size, 0);
}

bool HashMap::insert(const kmer_pair& kmer) {
    uint64_t hash = kmer.hash();
    for (uint64_t probe = 0; probe < size(); ++probe) {
        uint64_t slot = (hash + probe) % size();
        // Determine which process owns the slot
        int owner = determine_owner(slot);

        if (owner == upcxx::rank_me()) {
            // Local insertion
            if (!slot_used(slot)) {
                write_slot(slot, kmer);
                return true;
            }
        } else {
            // Remote insertion
            bool success = upcxx::rpc(owner, [this, slot, &kmer] {
                return request_slot(slot) && write_slot(slot, kmer);
            }).wait();
            if (success) {
                return true;
            }
        }
    }
    return false;
}

bool HashMap::find(const pkmer_t& key_kmer, kmer_pair& val_kmer) {
    uint64_t hash = key_kmer.hash();
    for (uint64_t probe = 0; probe < size(); ++probe) {
        uint64_t slot = (hash + probe) % size();
        int owner = determine_owner(slot);

        if (owner == upcxx::rank_me()) {
            // Local search
            if (slot_used(slot) && data[slot].kmer == key_kmer) {
                val_kmer = read_slot(slot);
                return true;
            }
        } else {
            // Remote search
            kmer_pair found_kmer = upcxx::rpc(owner, [this, slot] {
                return read_slot(slot);
            }).wait();

            if (found_kmer.kmer == key_kmer) {
                val_kmer = found_kmer;
                return true;
            }
        }
    }
    return false;
}


bool HashMap::slot_used(uint64_t slot) { return used[slot] != 0; }

void HashMap::write_slot(uint64_t slot, const kmer_pair& kmer) { data[slot] = kmer; }

kmer_pair HashMap::read_slot(uint64_t slot) { return data[slot]; }

bool HashMap::request_slot(uint64_t slot) {
    if (used[slot] != 0) {
        return false;
    } else {
        used[slot] = 1;
        return true;
    }
}

size_t HashMap::size() const noexcept { return my_size; }
