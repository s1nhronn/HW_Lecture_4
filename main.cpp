#include <condition_variable>
#include <cstddef>
#include <functional>
#include <iostream>
#include <mutex>
#include <queue>
#include <random>
#include <syncstream>
#include <thread>
#include <vector>
#include <algorithm>

struct task_t
{
  void operator()();
};

struct th_pool_t
{
public:
  using f_t = std::function< void(void) >;
  explicit th_pool_t(unsigned int k);
  ~th_pool_t();
  void submit(f_t f);
  void wait();
  void pause();
  void resume();

private:
  void execute(size_t i);
  std::queue< f_t > tasks_;
  std::mutex mx_;
  std::condition_variable cd_;
  std::condition_variable wait_cd_;
  size_t counter_ = 0;
  bool stop_{false};
  bool is_pause_{false};
  size_t threadsCount_;
  static constexpr size_t k_ = 2;
  std::vector< std::thread > ths_;
  std::vector< std::queue< f_t > > localTasks_;
  std::mutex ltmx_;
};

int main()
{
  th_pool_t pool(5);
  for (size_t i = 0; i < 30; ++i)
  {
    pool.submit(task_t{});
  }
  pool.wait();
  std::cout << "123\n";
} // pool.~th_poop_t()

void th_pool_t::wait()
{
  std::unique_lock< std::mutex > lck(mx_);
  auto proceed = [&]()
  {
    return !counter_;
  };
  wait_cd_.wait(lck, proceed);
}

void th_pool_t::pause()
{
  std::scoped_lock< std::mutex > lck(mx_);
  is_pause_ = true;
}

void th_pool_t::resume()
{
  {
    std::scoped_lock< std::mutex > lck(mx_);
    is_pause_ = false;
  }
  cd_.notify_all();
}

void th_pool_t::submit(f_t f)
{
  {
    std::scoped_lock< std::mutex > lck(mx_);
    tasks_.push(std::move(f));
    ++counter_;
  }
  cd_.notify_one();
}

th_pool_t::th_pool_t(unsigned k):
  threadsCount_(std::max(1u, k)),
  localTasks_(threadsCount_)
{
  auto thf = [this](size_t pos)
  {
    execute(pos);
  };
  for (size_t i = 0; i < threadsCount_; ++i)
  {
    ths_.emplace_back(thf, i);
  }
}

th_pool_t::~th_pool_t()
{
  {
    std::scoped_lock< std::mutex > lck(mx_);
    stop_ = true;
    is_pause_ = false;
  }
  cd_.notify_all();
  for (auto&& th : ths_)
  {
    th.join();
  }
}

void task_t::operator()()
{
  thread_local std::mt19937 gen(std::random_device{}());
  std::uniform_int_distribution< int > dist(100, 3000);

  std::this_thread::sleep_for(std::chrono::milliseconds(dist(gen)));
  std::osyncstream(std::cout) << "!\n";
}

void th_pool_t::execute(size_t pos)
{
  std::unique_lock< std::mutex > lck(mx_);
  do
  {
    auto proceed = [&]()
    {
      if (stop_ && !counter_)
      {
        return true;
      }
      if (is_pause_)
      {
        return false;
      }
      if (!tasks_.empty())
      {
        return true;
      }
      std::scoped_lock< std::mutex > localLock(ltmx_);
      for (size_t i = 0; i < localTasks_.size(); ++i)
      {
        if (!localTasks_[i].empty())
        {
          return true;
        }
      }
      return false;
    };
    cd_.wait(lck, proceed);

    if (tasks_.empty())
    {
      std::scoped_lock< std::mutex > localLock(ltmx_);
      bool isStolen = false;
      for (size_t offset = 1; offset < threadsCount_; ++offset)
      {
        size_t victim = (pos + offset) % threadsCount_;
        if (!localTasks_[victim].empty())
        {
          localTasks_[pos].push(std::move(localTasks_[victim].front()));
          localTasks_[victim].pop();
          isStolen = true;
          break;
        }
      }
      if (!isStolen)
      {
        continue;
      }
    }
    else
    {
      size_t b = std::max(size_t{1}, tasks_.size() / (k_ * threadsCount_));

      std::scoped_lock< std::mutex > localLock(ltmx_);
      for (size_t i = 0; i < b; ++i)
      {
        localTasks_[pos].push(std::move(tasks_.front()));
        tasks_.pop();
      }

      if (b > 1)
      {
        cd_.notify_all();
      }
    }

    lck.unlock();

    try
    {
      auto cond = [&]()
      {
        return !is_pause_ || stop_;
      };
      while (true)
      {
        lck.lock();
        cd_.wait(lck, cond);
        lck.unlock();

        std::unique_lock< std::mutex > localLock(ltmx_);
        if (localTasks_[pos].empty())
        {
          break;
        }

        auto task = std::move(localTasks_[pos].front());
        localTasks_[pos].pop();

        localLock.unlock();
        task();
        {
          std::scoped_lock< std::mutex > s(mx_);
          --counter_;
        }
      }
    }
    catch (...)
    {
      {
        std::scoped_lock< std::mutex, std::mutex > s(mx_, ltmx_);
        --counter_;
        std::queue< f_t >& lq = localTasks_[pos];
        while (!lq.empty())
        {
          tasks_.push(std::move(lq.front()));
          lq.pop();
        }
      }
    }

    lck.lock();
    if (!counter_)
    {
      wait_cd_.notify_all();
    }
  }
  while (!stop_ || counter_ > 0);
} // lck.~unique_lock() -> unlock()
