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
        implementation 'org.astraea:app:0.0.1-SNAPSHOT'
    }
}
```

### Astraea Dispatcher實作
1. [Smooth Dispatcher](smooth_dispatcher.md):  通過收集多metrics數據，結合熵權法與AHP進行節點狀況評估。再根據評估結果，使用smooth weight round-robin進行資料的調配。