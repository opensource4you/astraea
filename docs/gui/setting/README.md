### 設定 Astraea GUI

Astraea GUI 使用 Kafka APIs 與 JMX APIs 來協助管理叢集，因此使用 Astraea GUI 的第一步是先設定`bootstrap servers` 和 `jmx port`，如下圖：

![setting](setting.png)

`bootstrap servers` 是必要的資訊，它能給予 Astraea GUI 發送 admin request 的能力。
`jmx port` 則是選填的資訊，如果您的叢集有開啟對外的`jmx`服務的話，建議您在此處設定`jmx port`，如此 Astraea GUI 就能提供豐富的`metrics`查詢功能。
最後，當您設定好上述資訊後，千萬別忘了點擊`CHECK`來檢查填寫的資訊，如果該些資訊能提供正常的連線服務，下方`console`會顯示出成功連線的資訊。