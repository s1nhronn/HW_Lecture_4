#include <condition_variable>
#include <functional>
#include <iostream>
#include <mutex>
#include <queue>
#include <syncstream>
#include <thread>

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

private:
  void execute();
  std::queue< f_t > tasks_;
  std::mutex mx_;
  std::condition_variable cd_;
  std::condition_variable wait_cd_;
  size_t counter = 0;
  bool stop_{false};
  std::vector< std::thread > ths_;
};

int main()
{
  th_pool_t pool(5);
  for (size_t i = 0; i < 10; ++i)
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
    return !counter || stop_;
  };
  wait_cd_.wait(lck, proceed);
}

void th_pool_t::submit(f_t f)
{
  mx_.lock();
  tasks_.push(std::move(f));
  mx_.unlock();
  cd_.notify_one();
}

th_pool_t::th_pool_t(unsigned int k)
{
  auto thf = [this]()
  {
    execute();
  };
  for (; k; --k)
  {
    ths_.emplace_back(thf);
  }
}

th_pool_t::~th_pool_t()
{
  mx_.lock();
  stop_ = true;
  mx_.unlock();
  cd_.notify_all();
  for (auto&& th : ths_)
  {
    th.join();
  }
}

void task_t::operator()()
{
  using namespace std::chrono_literals;
  std::this_thread::sleep_for(200ms);
  std::osyncstream(std::cout) << "!\n";
}

void th_pool_t::execute()
{
  std::unique_lock< std::mutex > lck(mx_);
  do
  {
    auto proceed = [&]()
    {
      return !tasks_.empty() || stop_;
    };
    cd_.wait(lck, proceed);
    auto task = std::move(tasks_.front());
    tasks_.pop();
    lck.unlock();
    try
    {
      task();
      lck.lock();
      // TODO:
    }
    catch (...)
    {}
    lck.lock();
    if (tasks_.empty())
    {
      wait_cd_.notify_one();
    }
  } while (!stop_ || !tasks_.empty());
} // lck.~unique_lock() -> unlock()
