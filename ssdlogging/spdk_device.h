#pragma once

#include "spdk/stdinc.h"
#include "spdk/nvme.h"
#include "spdk/vmd.h"
#include "spdk/env.h"
#include "cstdio"
#include "string"

#include "vector"
#include "thread"
#include "sys/syscall.h"
#include "functional"

namespace ssdlogging{

const uint64_t SPDK_MAX_IO_SIZE = 1ull<<32;

struct NSEntry { // namespace
	struct spdk_nvme_ctrlr	*ctrlr;
	struct spdk_nvme_ns	*ns;
	struct NSEntry		*next;
	struct spdk_nvme_qpair	**qpair;
	uint32_t sector_size;
	uint64_t capacity; //GB
};

struct SPDKInfo{
	struct spdk_nvme_ctrlr *controller = nullptr;
	int num_ns;  // number of namespace
	struct NSEntry *namespaces = nullptr;
	char name[200];
	std::string addr;
	uint32_t num_io_queues;
};

struct SPDKData {
	struct NSEntry	*ns_entry;
	char *buf;
	bool is_completed;
};

extern SPDKInfo *g_spdk_;

extern struct SPDKInfo * InitSPDK(std::string device_addr, int logging_queue_num);


}