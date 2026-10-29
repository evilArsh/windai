//    pub struct Hub {
//        shared: Mutex<Delivery>,
//    }
//    struct Delivery { subscribers: Vec<Registered> }   // 每条连接：scope + 帧队列

//    pub subscribe(scope, since_pos) ->
//     //    临界区 { boundary = storage.event_log_boundary()
//     //             blocks   = 快照()            // SQL 定集合 + 累加器覆盖内容
//     //             注册订阅 }
//     //    锁外   { history = 按 pos 区间查日志并补值 }
//     //    返回   { history + blocks 作为首帧，之后走实时 }

//    impl BuiltinObserver for Hub {
//        on_event(meta, event)
//    }
