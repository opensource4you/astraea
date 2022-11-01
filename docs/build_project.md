### Build and Test Project

#### 各模組說明 ####

- app: 處理與web server，成本分析，覆載平衡等功能之模組
- common: 有關叢集，客戶端等分析，管理與平衡等功能之模組
- etl: 轉換 csv 檔，再匯入 delta 並使串接部份有更好的平行化處理之模組
- gui: 與圖形化界面功能相關的模組
- it: 針對專案測試所提供的叢集環境

以下示範如何建構與測試本專案

#### 建構專案並忽略測試 ####
    ./gradlew clean build -x test

#### 清理並測試整個專案 ####
    ./gradlew cleanTest test

#### 各模組測試指令 ####
清理test，並測試 common 模組

    ./gradlew cleanTest common:test

#### 清理先建構之資料
    ./gradlew clean 

#### 直接運行 main class ####
如果要取得 app 的版本資訊，可以透過以下指令直接執行

    ./gradlew clean app:run -version

如果要執行gui界面，可以透過以下指令

    ./gradlew clean gui:run
