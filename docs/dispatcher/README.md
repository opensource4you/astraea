Astraea Dispatcher 中文文件
===
Astraea Dispatcher 是強大且高效率的 Kafka Partitioner 實作，提供豐富且彈性的叢集負載選項，從寫入端動態維持使用者定義後的負載平衡.

### 通過gradle引入Astraea
在build.gradle中添加以下內容
```gradle
repositories {
    maven {
        url = "https://maven.pkg.github.com/skiptests/astraea"
        credentials {
            username = System.getenv("GITHUB_ACTOR")
            password = System.getenv("GITHUB_TOKEN")
        }
    }
    
    dependencies {
        implementation 'org.astraea:astraea-common:0.0.1-SNAPSHOT'
    }
}
```

### Astraea Dispatcher實作
1. [Smooth Dispatcher](smooth_dispatcher.md):  通過收集多metrics數據，結合熵權法與AHP進行節點狀況評估。再根據評估結果，使用 smooth weight round-robin 進行資料的調配。
1. [Strict Cost Dispatcher](./strict_cost_dispatcher.md): 收集使用者自定義的效能指標，使用效能指標為節點打分。再根據加權分數，使用 smooth weight round-robin 進行資料調配。

### Astraea Dispatcher 實驗

experiments 資料夾中收錄不同版本的實驗紀錄，主要使用 [performance tool](../performance_benchmark.md) 測試並紀錄數據。

* [2022 Aug28](experiments/StrictCostDispatcher_1.md), 測試 [Strict Cost Partitioner](./strict_cost_dispatcher.md) (Astraea revision: [75bcc3faa39864d5ec5f5ed530346184e79fc0c9](https://github.com/skiptests/astraea/tree/75bcc3faa39864d5ec5f5ed530346184e79fc0c9))