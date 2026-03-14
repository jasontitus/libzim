/*
 * Copyright (C) 2019-2020 Matthieu Gautier <mgautier@kymeria.fr>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU  General Public License as published by
 * the Free Software Foundation; either version 3 of the License, or
 * any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301, USA.
 */

#ifndef OPENZIM_LIBZIM_WORKERS_H
#define OPENZIM_LIBZIM_WORKERS_H

#include "creatordata.h"

#include <atomic>
#include <mutex>
#include <condition_variable>

namespace zim {
namespace writer {

class Task {
  public:
    Task() = default;
    virtual ~Task() = default;

    virtual void run(CreatorData* data) = 0;
};

template<class T>
class TrackableTask: public Task {
  public:
    TrackableTask(const TrackableTask&) = delete;
    TrackableTask& operator=(const TrackableTask&) = delete;
    TrackableTask() { ++waitingTaskCount; }
    virtual ~TrackableTask() {
      if (--waitingTaskCount == 0) {
        std::lock_guard<std::mutex> lock(taskCountMutex());
        taskCountCV().notify_all();
      }
    }

    static void waitNoMoreTask(const CreatorData* data) {
      std::unique_lock<std::mutex> lock(taskCountMutex());
      taskCountCV().wait(lock, [data]() {
        return waitingTaskCount.load() == 0 || data->isErrored();
      });
    }

  private:
    static std::atomic<unsigned long> waitingTaskCount;

    static std::mutex& taskCountMutex() {
      static std::mutex m;
      return m;
    }
    static std::condition_variable& taskCountCV() {
      static std::condition_variable cv;
      return cv;
    }
};

template<class T>
std::atomic<unsigned long> zim::writer::TrackableTask<T>::waitingTaskCount(0);

void* taskRunner(void* data);
void* clusterWriter(void* data);

}
}

#endif // OPENZIM_LIBZIM_WORKERS_H
