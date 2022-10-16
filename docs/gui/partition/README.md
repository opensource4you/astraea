### partition

可以在此頁面取得各`topic`的`partition`資訊，並搬移`partition`至指定`broker`


![](partition_partition_1.png)
*顯示出所有`topic`的個`partition`資料*

![](partition_partition_2.png)
*指定所有名稱結尾是1的`topic`*


搬移`partition`時，需要先選擇特定`topic`，此時會列出此`topic`旗下所有的`partition`

![](partition_alter_1.png)

選定要搬移的`partition`與其搬移`broker`跟`truncate`數，點擊`ALTER`後即可看到
此工具執行搬移後的結果，如果`partition`為空，則會把所有的`partition`搬移

![](partition_alter_2.png)
*把所有`partition`移至`id`為 1001的`broker`，並且更新其`earliest offset`為100*