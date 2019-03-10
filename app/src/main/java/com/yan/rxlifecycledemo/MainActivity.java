package com.yan.rxlifecycledemo;

import android.arch.lifecycle.Lifecycle;
import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.util.Log;
import com.yan.rxlifehelper.RxLifeHelper;
import io.reactivex.Flowable;
import io.reactivex.FlowableSubscriber;
import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.Scheduler;
import io.reactivex.Single;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import io.reactivex.plugins.RxJavaPlugins;
import io.reactivex.schedulers.Schedulers;
import java.util.concurrent.TimeUnit;
import org.reactivestreams.Subscription;

public class MainActivity extends AppCompatActivity {

  @Override protected void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);
    setContentView(R.layout.activity_main);
  }

  @Override protected void onResume() {
    super.onResume();

    for (int i = 0; i < 100; i++) {
      Single.timer(1000, TimeUnit.MILLISECONDS)
          .compose(RxLifeHelper.<Long>bindUntilLifeEvent(this, Lifecycle.Event.ON_PAUSE))
          .subscribeOn(Schedulers.newThread())
          .subscribe(new Consumer<Long>() {
            @Override public void accept(Long aLong) throws Exception {
              Log.e("RxLifeHelper", "interval ---------");
              Observable.just(111)
                  .compose(RxLifeHelper.<Integer>bindUntilLifeEvent(MainActivity.this,
                      Lifecycle.Event.ON_PAUSE))
                  .subscribe(new Observer<Integer>() {
                    @Override public void onSubscribe(Disposable d) {
                      Log.e("onSubscribe", "onSubscribe: " + d.isDisposed());
                    }

                    @Override public void onNext(Integer integer) {
                      Log.e("onSubscribe", "onNext: ");
                    }

                    @Override public void onError(Throwable e) {
                      Log.e("onSubscribe", "onError: " + e);
                    }

                    @Override public void onComplete() {
                      Log.e("onSubscribe", "onComplete: ");
                    }
                  });
            }
          }, new Consumer<Throwable>() {
            @Override public void accept(Throwable throwable) throws Exception {
              Log.e("getData", "accept: " + throwable);
            }
          });
    }

    Observable.interval(1000, TimeUnit.MILLISECONDS)
        .compose(RxLifeHelper.<Long>bindUntilLifeEvent(this, Lifecycle.Event.ON_PAUSE))
        .subscribeOn(Schedulers.io())
        .subscribe(new Consumer<Long>() {
          @Override public void accept(Long aLong) throws Exception {
            Log.e("RxLifeHelper", "interval ---------");
          }
        }, new Consumer<Throwable>() {
          @Override public void accept(Throwable throwable) throws Exception {
            Log.e("getData", "accept: " + throwable);
          }
        });

    // 1111111111111 将不会 被打印
    getData("111111111111111111111111111111");
    getData("222222222222222222222222222222");
  }

  private void getData(final String data) {
    Flowable.timer(2000, TimeUnit.MILLISECONDS).subscribe(new FlowableSubscriber<Long>() {
      @Override public void onSubscribe(Subscription s) {
        s.request(Integer.MAX_VALUE);
        Log.e("getDatagetData", "onSubscribe ");
      }

      @Override public void onNext(Long aLong) {
        Log.e("getDatagetData", "onNextonNext " + aLong);
      }

      @Override public void onError(Throwable t) {

      }

      @Override public void onComplete() {

      }
    });
    Single.timer(1000, TimeUnit.MILLISECONDS)
        .compose(RxLifeHelper.<Long>bindFilterTag("getData"))
        .compose(RxLifeHelper.<Long>bindUntilLifeEvent(this, Lifecycle.Event.ON_PAUSE))
        .doOnDispose(new Action() {
          @Override public void run() throws Exception {
            Log.e("doOnDispose", "run: doOnDisposedoOnDispose");
          }
        })
        .subscribe(new Consumer<Long>() {
          @Override public void accept(Long aLong) throws Exception {
            Log.e("RxLifeHelper", "event " + data);
          }
        }, new Consumer<Throwable>() {
          @Override public void accept(Throwable throwable) throws Exception {
            Log.e("getData", "accept: " + throwable);
          }
        });
  }
}
