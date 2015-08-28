package com.app.practice;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * User: kaushik Date: 16/11/13 Time: 7:49 PM
 */

public abstract class Parallel<T>
{

  public void doParallelOp(Collection<T> items) throws InterruptedException
  {
    List<Thread> threads = new ArrayList<Thread>(items.size());

    for (T item : items)
    {
      final T finalItem = item;
      Thread thread = new Thread(new Runnable()
      {
        @Override
        public void run()
        {
          System.out.println(" started capture by thread: " + Thread.currentThread().getName());
          try
          {
            executeOnItem(finalItem);
          }
          catch (Exception e)
          {
            e.printStackTrace();
          }
        }
      });
      threads.add(thread);
      thread.start();
    }

    for (Thread thread : threads)
    {
      thread.join();
    }
  }

  protected abstract void executeOnItem(T finalItem) throws IOException, InterruptedException;
}