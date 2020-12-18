# RxLife

### 轻量级的RxJava生命周期管理库
### A light library which can bind observables to the lifecycle of Activity or Fragment or View in a non-invasive way.

[![](https://img.shields.io/badge/platform-android-brightgreen.svg)](https://developer.android.com/index.html) 
[ ![API](https://img.shields.io/badge/API-19+-blue.svg?style=flat-square) ](https://developer.android.com/about/versions/android-4.0.html)
[ ![License](http://img.shields.io/badge/License-Apache%202.0-blue.svg?style=flat-square) ](http://www.apache.org/licenses/LICENSE-2.0)

### 为什么有这个库？
### 最近想找一个好用的RxJava生命周期管理库，找到一些比较好的开源项目，比如 [trello/RxLifecycle](https://github.com/trello/RxLifecycle) ， [dhhAndroid/RxLife](https://github.com/dhhAndroid/RxLife) ，[liujingxing/rxjava-RxLife](https://github.com/liujingxing/rxjava-RxLife) 以及[genius158/RxLifeHelper](https://github.com/genius158/RxLifeHelper)。trello/RxLifecycle和dhhAndroid/RxLife貌似不支持监听View的生命周期；liujingxing/rxjava-RxLife库虽支持监听View, 但是在使用过程中不能返回原类型的Observable对象；而genius158/RxLifeHelper是一个不错的库，不仅支持监听View，还拓展了根据Tag管理生命周期和LiveData，但是该库是基于androidx的，目前我所做的项目基本上是使用support库的项目，所以fork了genius158/RxLifeHelper项目并对代码进行了修改和简化。



## Install

#### Latest Version：[![Download](https://api.bintray.com/packages/z-p-j/maven/RxLife/images/download.svg?version-1.0.1)](https://bintray.com/z-p-j/maven/RxLife/1.0.1/link)
```groovy
    // RxJava2
    implementation 'io.reactivex.rxjava2:rxjava:2.2.17'
	// RxAndroid
    implementation 'io.reactivex.rxjava2:rxandroid:2.1.1'
    // RxLife
    implementation 'com.zpj.rxlife:RxLife:latest_version'

```
## How To Use?（[The demo](https://github.com/Z-P-J/RxLife/tree/master/app)）
```java
    // 简单使用
    Observable.timer(10000, TimeUnit.MILLISECONDS)
        // 绑定Tag
         .compose(RxLife.<Long>bindTag(object))
         // 绑定生命周期
         .compose(RxLife.<Long>bindLifeOwner(this, Lifecycle.Event.ON_PAUSE))
         // 在配合生命周期的前提下，配合LiveData
         .compose(RxLife.<Long>bindLifeOwnerLive(this, Lifecycle.Event.ON_PAUSE))
         .subscribe(new Consumer<Long>() {
           @Override public void accept(Long aLong) throws Exception {
             Log.e("RxLifeHelper", "event " + data);
           }
         });

    // 所用绑定方法
    RxLife.bindLifeOwner(lifecycleOwner);
    RxLife.bindLifeOwner(lifecycleOwner, Lifecycle.Event);
    RxLife.bindView(view);
    RxLife.bindRootView(view);
    RxLife.bindActivity(activity);
    RxLife.bindContext(context);
    RxLife.bindTag(object);
    RxLife.bindLifeOwnerLive(lifecycleOwner);
    RxLife.bindLifeOwnerLive(lifecycleOwner, Lifecycle.Event);
    RxLife.bindActivityLive(activity);
    RxLife.bindContextLive(context);

    // 根据Tag移除，可在任何地方调用
    RxLife.removeByTag(object);
```

## License
```
   Copyright 2019 Z-P-J

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
```
