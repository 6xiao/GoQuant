# GoQuant
simple algorithmic analyze and strategy trading

一个未完成的个人项目，用以量化投资的回测和实盘交易，当前仅开源回测部分。

如需要交易部分，请自行写模块接入通讯组件即可。

组件之间通讯需要使用同作者写的 [SimpleMsgChan](https://github.com/6xiao/go/tree/master/SimpleMsgChan)

由以下模块组成：

+ HD ： 历史数据模块，依赖 MySql
+ OS ： 拆单模块
+ ST ： 策略模块
+ VE ： 虚拟撮合模块
