#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <chrono>
#include <iostream>
#include <string>
#include <vector>

#include "stdint.h"

namespace ssdlogging {

class StatIO {
 private:
  struct io_stats {
    /* # of sectors read */
    unsigned long rd_sectors;
    /* # of sectors written */
    unsigned long wr_sectors;
    /* # of read operations issued to the device */
    unsigned long rd_ios;
    /* # of read requests merged */
    unsigned long rd_merges;
    /* # of write operations issued to the device */
    unsigned long wr_ios;
    /* # of write requests merged */
    unsigned long wr_merges;
    /* Time of read requests in queue */
    unsigned long rd_ticks;
    /* Time of write requests in queue */
    unsigned int wr_ticks;
    /* # of I/Os in progress */
    unsigned int ios_pgr;
    /* # of ticks total (for this device) for I/O */
    unsigned int tot_ticks;
    /* # of ticks requests spent in queue */
    unsigned int rq_ticks;
  };
  const std::string DISKSTATS = "/proc/diskstats";
  const int MAX_NAME_LEN = 128;
  std::string target_name = "sdb";
  struct io_stats sdev_ini;
  struct io_stats sdev[2];

  void read_diskstats_stat() {
    FILE *fp;
    char line[256], dev_name[MAX_NAME_LEN];
    unsigned int ios_pgr, tot_ticks, rq_ticks, wr_ticks;
    unsigned long rd_ios, rd_merges_or_rd_sec, rd_ticks_or_wr_sec, wr_ios;
    unsigned long wr_merges, rd_sec_or_wr_ios, wr_sec;
    char *ioc_dname;
    unsigned int major, minor;
    if ((fp = fopen(DISKSTATS.c_str(), "r")) == NULL) return;
    while (fgets(line, sizeof(line), fp) != NULL) {
      /* major minor name rio rmerge rsect ruse wio wmerge wsect wuse running
       * use aveq */
      int i =
          sscanf(line, "%u %u %s %lu %lu %lu %lu %lu %lu %lu %u %u %u %u",
                 &major, &minor, dev_name, &rd_ios, &rd_merges_or_rd_sec,
                 &rd_sec_or_wr_ios, &rd_ticks_or_wr_sec, &wr_ios, &wr_merges,
                 &wr_sec, &wr_ticks, &ios_pgr, &tot_ticks, &rq_ticks);
      if (!strcmp(dev_name, target_name.c_str())) {
        sdev_ini.rd_ios = rd_ios;
        sdev_ini.rd_merges = rd_merges_or_rd_sec;
        sdev_ini.rd_sectors = rd_sec_or_wr_ios;
        sdev_ini.rd_ticks = rd_ticks_or_wr_sec;
        sdev_ini.wr_ios = wr_ios;
        sdev_ini.wr_merges = wr_merges;
        sdev_ini.wr_sectors = wr_sec;
        sdev_ini.wr_ticks = wr_ticks;
        sdev_ini.ios_pgr = ios_pgr;
        sdev_ini.tot_ticks = tot_ticks;
        sdev_ini.rq_ticks = rq_ticks;
      } else {
        continue;
      }
    }
    fclose(fp);
  }

  void read_diskstats_stat(int curr) {
    FILE *fp;
    char line[256], dev_name[MAX_NAME_LEN];

    unsigned int ios_pgr, tot_ticks, rq_ticks, wr_ticks;
    unsigned long rd_ios, rd_merges_or_rd_sec, rd_ticks_or_wr_sec, wr_ios;
    unsigned long wr_merges, rd_sec_or_wr_ios, wr_sec;
    char *ioc_dname;
    unsigned int major, minor;

    if ((fp = fopen(DISKSTATS.c_str(), "r")) == NULL) return;

    while (fgets(line, sizeof(line), fp) != NULL) {
      /* major minor name rio rmerge rsect ruse wio wmerge wsect wuse running
       * use aveq */
      int i =
          sscanf(line, "%u %u %s %lu %lu %lu %lu %lu %lu %lu %u %u %u %u",
                 &major, &minor, dev_name, &rd_ios, &rd_merges_or_rd_sec,
                 &rd_sec_or_wr_ios, &rd_ticks_or_wr_sec, &wr_ios, &wr_merges,
                 &wr_sec, &wr_ticks, &ios_pgr, &tot_ticks, &rq_ticks);
      if (!strcmp(dev_name, target_name.c_str())) {
        sdev[curr].rd_ios = rd_ios;
        sdev[curr].rd_merges = rd_merges_or_rd_sec;
        sdev[curr].rd_sectors = rd_sec_or_wr_ios;
        sdev[curr].rd_ticks = rd_ticks_or_wr_sec;
        sdev[curr].wr_ios = wr_ios;
        sdev[curr].wr_merges = wr_merges;
        sdev[curr].wr_sectors = wr_sec;
        sdev[curr].wr_ticks = wr_ticks;
        sdev[curr].ios_pgr = ios_pgr;
        sdev[curr].tot_ticks = tot_ticks;
        sdev[curr].rq_ticks = rq_ticks;
      } else {
        continue;
      }
    }
    fclose(fp);
  }

 public:
  StatIO(std::string target) : target_name(target) {
    memset(&sdev_ini, 0, sizeof(struct io_stats));
    read_diskstats_stat();
  }

  void GetTotalMBytes(unsigned long long &rd_sec, unsigned long long &wr_sec) {
    for (int i = 0; i < 2; i++) memset(&sdev[i], 0, sizeof(struct io_stats));
    int curr = 1;
    int fctr = 2048;
    read_diskstats_stat(curr);

    rd_sec = sdev[curr].rd_sectors - sdev_ini.rd_sectors;
    if ((sdev[curr].rd_sectors < sdev_ini.rd_sectors) &&
        (sdev_ini.rd_sectors <= 0xffffffff)) {
      rd_sec &= 0xffffffff;
    }
    wr_sec = sdev[curr].wr_sectors - sdev_ini.wr_sectors;
    if ((sdev[curr].wr_sectors < sdev_ini.wr_sectors) &&
        (sdev_ini.wr_sectors <= 0xffffffff)) {
      wr_sec &= 0xffffffff;
    }
    rd_sec /= fctr;
    wr_sec /= fctr;
  }
};

}  // namespace ssdlogging