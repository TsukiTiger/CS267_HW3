#pragma once

#include "kmer_t.hpp"
#include <upcxx/upcxx.hpp>

struct HashMap {
    //std::vector<kmer_pair> data;
    //std::vector<int> used;

    upcxx::global_ptr<kmer_pair>* data_pt;    
    upcxx::global_ptr<int>* used_pt; 
    upcxx::atomic_domain<int> ad; 
	
    int srank;
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
    upcxx::future<int> request_slot(uint64_t slot);
    bool slot_used(uint64_t slot);
};

size_t HashMap::size() const noexcept { return my_size; }

HashMap::HashMap(size_t size) {
    my_size = size;
    srank = size/upcxx::rank_n()+1;

    upcxx::dist_object<upcxx::global_ptr<kmer_pair>> data(upcxx::new_array<kmer_pair>(srank));
    upcxx::dist_object<upcxx::global_ptr<int>> used(upcxx::new_array<int>(srank));
    
    data_pt = new upcxx::global_ptr<kmer_pair>[upcxx::rank_n()];
    used_pt = new upcxx::global_ptr<int>[upcxx::rank_n()];
    
    // load data
    upcxx::barrier();    
    for (int rank = 0; rank < upcxx::rank_n(); rank++){
        data_pt[rank] = data.fetch(rank).wait();
        used_pt[rank] = used.fetch(rank).wait();
    }
    upcxx::barrier();
    
    ad = upcxx::atomic_domain<int>({upcxx::atomic_op::load,upcxx::atomic_op::fetch_add});
}

bool HashMap::insert(const kmer_pair& kmer) {

    uint64_t hash = kmer.hash();
    uint64_t probe = 0;
    bool inserted = false;

    while (!inserted && probe < size()){
     uint64_t slot = (hash + probe) % size();
     probe += 1;

     inserted = request_slot(slot).wait()>0 ? false : true;
     if (inserted) {write_slot(slot, kmer);}
    }

    return inserted;
}

bool HashMap::find(const pkmer_t& key_kmer, kmer_pair& val_kmer) {
    uint64_t hash = key_kmer.hash();
    uint64_t probe = 0;
    bool found = false;

    while(!found && probe < size()){
	uint64_t slot = (hash + probe) % size();
	probe += 1 ;

	val_kmer = read_slot(slot);
        if (val_kmer.kmer == key_kmer) {
                found = true;
            }
   }
    return found;
}




bool HashMap::slot_used(uint64_t slot) { 
    uint64_t bin = slot/srank;
    uint64_t shift = slot%srank;
    upcxx::global_ptr<int> remote_slot = used_pt[bin] + shift;
    return ad.load(remote_slot,std::memory_order_relaxed).wait() != 0; 
}

upcxx::future<int> HashMap::request_slot(uint64_t slot) {
    uint64_t bin = slot/srank;
    uint64_t shift = slot%srank;
    upcxx::global_ptr<int> remote_slot = used_pt[bin] + shift;
    return ad.fetch_add(remote_slot, 1, std::memory_order_relaxed);
}

kmer_pair HashMap::read_slot(uint64_t slot) {
    uint64_t bin = slot/srank;
    uint64_t shift = slot%srank;
    upcxx::global_ptr<kmer_pair> remote_kmer_slot = data_pt[bin] + shift;
    return upcxx::rget(remote_kmer_slot).wait();
}

void HashMap::write_slot(uint64_t slot, const kmer_pair& kmer) {
    uint64_t bin = slot/srank;
    uint64_t shift = slot%srank;
    upcxx::global_ptr<kmer_pair> remote_kmer_slot = data_pt[bin] + shift;
    auto fut = upcxx::rput(kmer,remote_kmer_slot);
}




