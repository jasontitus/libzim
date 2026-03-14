/*
 * Copyright (C) 2019-2020 Matthieu Gautier <mgautier@kymeria.fr>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * is provided AS IS, WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, and
 * NON-INFRINGEMENT.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
 *
 */

#include "workers.h"
#include "cluster.h"
#include "creatordata.h"

#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif

namespace zim
{
  namespace writer
  {

    void* taskRunner(void* arg) {
      auto creatorData = static_cast<zim::writer::CreatorData*>(arg);
      try {
        while(true) {
          std::shared_ptr<Task> task;
          {
            auto& q = creatorData->taskList;
            std::unique_lock<std::mutex> lock(q.m_queueMutex);
            q.m_pushCV.wait(lock, [&]() {
              return !q.m_realQueue.empty() || creatorData->isErrored();
            });
            if (creatorData->isErrored()) {
              return nullptr;
            }
            task = q.m_realQueue.front();
            q.m_realQueue.pop();
          }
          creatorData->taskList.m_popCV.notify_one();
          if (!task) {
            return nullptr;
          }
          task->run(creatorData);
        }
      } catch (...) {
        creatorData->addError(std::current_exception());
      }
      return nullptr;
    }

    void* clusterWriter(void* arg) {
      auto creatorData = static_cast<zim::writer::CreatorData*>(arg);
      Cluster* cluster;
      try {
        while(true) {
          {
            std::unique_lock<std::mutex> lock(creatorData->m_clusterClosedMutex);
            creatorData->m_clusterClosedCV.wait(lock, [&]() {
              return creatorData->isErrored()
                  || (creatorData->clusterToWrite.getHead(cluster)
                      && (cluster == nullptr || cluster->isClosed()));
            });
          }
          if (creatorData->isErrored()) {
            return nullptr;
          }
          if (cluster == nullptr) {
            return nullptr;
          }
          creatorData->clusterToWrite.popFromQueue(cluster);
          cluster->setOffset(offset_t(lseek(creatorData->out_fd, 0, SEEK_CUR)));
          cluster->write(creatorData->out_fd);
          cluster->clear_data();
        }
      } catch(...) {
        creatorData->addError(std::current_exception());
      }
      return nullptr;
    }
  }
}
