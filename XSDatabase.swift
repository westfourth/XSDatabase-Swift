//
//  XSDatabse.swift
//  XSDatabase2
//
//  Created by xisi on 15/10/31.
//  Copyright © 2015年 xisi. All rights reserved.
//

import Foundation

//let SQLITE_STATIC = unsafeBitCast(0, sqlite3_destructor_type.self)      //  不安全
let SQLITE_TRANSIENT = unsafeBitCast(-1, sqlite3_destructor_type.self)

/// 回调
typealias database_stmt_block_t = ((index: Int, stmt: COpaquePointer) -> Void)?

/// 打印
func SQLitePrint(items: Any...) {
    if true {
        let s = "___SQLite "
        print(s, items)
    }
}

/**
需引入Objective-C桥接文件，并在桥接文件中导入sqlite头文件

改动:

1. 插入时，只准备一次
 */
class XSDatabase {
    
    private var db: COpaquePointer = nil
    /*
    注意sqlite3_config()非线程安全，且容易出现SQLITE_MISUSE
    SQLITE_MUTEX_RECURSIVE：同一个线程可以进入多次
    */
    private let transaction_mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST)

    
    deinit {
        sqlite3_mutex_free(transaction_mutex)
    }
    
    //  MARK: - 打开、关闭
    //_______________________________________________________________________________________________________________
    /// 打开数据库
    func open(file: String = ":memory:") {
        /*
        1. Single-thread.   - In this mode, all mutexes are disabled and SQLite is unsafe to use in more than a single thread at once.
        2. Multi-thread.    - In this mode, SQLite can be safely used by multiple threads provided that no single database connection is used simultaneously in two or more threads.
        3. Serialized.      - In serialized mode, SQLite can be safely used by multiple threads with no restriction.
        */
        let flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX
        if sqlite3_open_v2(file, &db, flags, nil) != SQLITE_OK {
            SQLitePrint("打开数据库失败: \(String.fromCString(sqlite3_errmsg(db)))")
            if sqlite3_close(db) != SQLITE_OK {
                SQLitePrint("关闭数据库失败: \(String.fromCString(sqlite3_errmsg(db)))")
            }
            return
        }
        SQLitePrint("打开数据库: \(String.fromCString(sqlite3_db_filename(db, "main")))")
        /*
        设置多个对象同时修改数据时，重试的超时时间。注意：在事务中无效，但开起事务的保留锁。
        如果不设置则会‘database is locked’
        */
        if sqlite3_busy_timeout(db, 60 * 1000) != SQLITE_OK {
            SQLitePrint("设置文件锁超时错误: \(String.fromCString(sqlite3_errmsg(db)))")
        }
    }
    
    /// 关闭数据库
    func close() {
        if sqlite3_close(db) != SQLITE_OK {
            SQLitePrint("关闭数据库失败: \(String.fromCString(sqlite3_errmsg(db)))")
        }
    }
    
    //  MARK: - 表的操作
    //_______________________________________________________________________________________________________________
    /**
    执行。支持多个语句: @"";
    
    执行插入记录语句时转义[']为['']，而不是[\']
    
    示例代码: 
    
        update company set code='1111'; update company set sortkey='2222'
    */
    func executeSQL(sql: String) -> Bool {
        if sqlite3_exec(db, sql, nil, nil, nil) != SQLITE_OK {
            SQLitePrint("执行失败: \(String.fromCString(sqlite3_errmsg(db)))")
            return false
        }
        return true
    }
    
    /**
    查询，(非异步。查询出错时，不回调)。 sqlite3_column_* 参数从0开始
     
    支持多表联合查询，不支持多个语句；如果有多个语句，则只有第一个起作用。
 
    示例代码:

        var array = [CompanyModel]()
        db.selectSQL("select * from company") { (index, stmt) -> Void in
            let model = CompanyModel()
            model.status = sqlite3_column_int(stmt, 0)
            model.code = String.fromCString(UnsafePointer(sqlite3_column_text(stmt, 1)))
            model.sortKey = String.fromCString(UnsafePointer(sqlite3_column_text(stmt, 2)))
            ......
            array.append(model)
        }
    */
    func selectSQL(sql: String, eachStmt: database_stmt_block_t) {
        var stmt: COpaquePointer = nil
        if sqlite3_prepare_v2(db, sql, -1, &stmt, nil) != SQLITE_OK {
            SQLitePrint("查询失败: \(String.fromCString(sqlite3_errmsg(db)))")
            return
        }
        var i: Int = 0
        while sqlite3_step(stmt) == SQLITE_ROW {
            eachStmt?(index: i++, stmt: stmt)   //  取值
        }
        if sqlite3_finalize(stmt) != SQLITE_OK {                  //  释放
            SQLitePrint("查询释放失败: \(String.fromCString(sqlite3_errmsg(db)))")
        }
    }
    
    /**
    增/改，(非异步。增/改出错时，不回调)。 sqlite3_bind_* 参数从1开始
     
        * replace into tb (ID, image, score) values (:ID, :image, :score)
        * replace into tb (ID, image, score) values (?, ?, ?)

    示例代码:
     
        let sql = "replace into company (status, code, sortKey, name, sortNo, pcode) values (?, ?, ?, ?, ?, ?)"
        db.updateSQL(sql, count: array.count) { (index, stmt) -> Void in
            let model = array[index] as CompanyModel
            sqlite3_bind_int(stmt, 1, model.status!)
            sqlite3_bind_text(stmt, 2, model.code!, -1, SQLITE_TRANSIENT)
            sqlite3_bind_text(stmt, 3, model.sortKey!, -1, SQLITE_TRANSIENT)
            ......
        }
    */
    func updateSQL(sql: String, count: Int = 1, eachStmt: database_stmt_block_t) {
        var stmt: COpaquePointer = nil
        if sqlite3_prepare_v2(db, sql, -1, &stmt, nil) != SQLITE_OK {
            SQLitePrint("增/改准备失败: \(String.fromCString(sqlite3_errmsg(db)))")
            return
        }
        var i: Int = 0
        while i < count {
            eachStmt?(index: i++, stmt: stmt)       //  绑定
            if sqlite3_step(stmt) != SQLITE_DONE {                      //  执行
                SQLitePrint("增/改失败: \(String.fromCString(sqlite3_errmsg(db)))")
            }
            if sqlite3_clear_bindings(stmt) != SQLITE_OK {              //  清除之前的绑定（内存泄露???）
                SQLitePrint("增/改清除绑定失败: \(String.fromCString(sqlite3_errmsg(db)))")
            }
            if sqlite3_reset(stmt) != SQLITE_OK {                       //  重置stmt，使其可以step执行
                SQLitePrint("增/改重置失败: \(String.fromCString(sqlite3_errmsg(db)))")
            }
        }
        if sqlite3_finalize(stmt) != SQLITE_OK {                        //  释放
            SQLitePrint("查询释放失败: \(String.fromCString(sqlite3_errmsg(db)))")
        }
    }
    
    
    //  MARK: - 其他
    //_______________________________________________________________________________________________________________
    /// 中断
    func interrupt() {
        sqlite3_interrupt(db)
    }
    
    /// false表示在用户事物中
    func autocommit() -> Bool {
        let result = sqlite3_get_autocommit(db)
        return result == 1
    }
}




//  MARK: - 扩展（只针对于executeSQL()）
//_______________________________________________________________________________________________________________
extension XSDatabase {
    //  MARK: - 分离、附加
    //_______________________________________________________________________________________________________________
    /**
    附加数据库
    
    databaseName与tableName都必须以字母或下划线开头（与C语言变量命名规范一样）
    
        1. a123.b123   -   表示a123数据库中的b123表
        2. 'a123.b123' -   表示main数据库中的a123.b123表
        3. "a123.b123" -   同2
    */
    func attach(file: String, name: String) {
        executeSQL("attach '\(file)' as \(name)")
    }
    
    /// 分离数据库
    func detach(name: String) {
        executeSQL("detach \(name)")
    }
    
    //  MARK: - 事务
    //_______________________________________________________________________________________________________________
    /**
    开始事务
    
    设置多线程中多对象并发事务，指定锁行为:
    
    0. deferred（其他可读写，默认）
    1. immediate（其他只读），其他数据库连接不可写入，也不可开启IMMEDIATE、EXCLUSIVE事务
    2. exclusive（不可读写），其他数据库连接只能读取数据
    */
    func beginTransaction() {
        sqlite3_mutex_enter(transaction_mutex);
        if executeSQL("begin") == false {
            sqlite3_mutex_leave(transaction_mutex);        //  多leave几次没关系，但只能enter一次
        }
    }
    
    /// 结束事务
    func commitTransaction() {
        if executeSQL("commit") == true {
            sqlite3_mutex_leave(transaction_mutex);
        }
    }
    
    /// 回滚事务
    func rollbackTransaction() {
        if executeSQL("rollback") == true {
            sqlite3_mutex_leave(transaction_mutex);
        }
    }
    
    //  MARK: - 事务保存点
    //_______________________________________________________________________________________________________________
    /// 保存点
    func savePoint(point: String) {
        executeSQL("savepoint \(point)")
    }
    
    /// 释放保存点
    func releasePoint(point: String) {
        executeSQL("release \(point)")
    }
    
    /// 回滚保存点
    func rollbackToPoint(point: String) {
        executeSQL("rollback to \(point)")
    }
}




//  MARK: - 其他扩展
//_______________________________________________________________________________________________________________
/// 其他
extension XSDatabase {
    
    /// 静态实例
    static let defaultDatabase: XSDatabase = {
        let database = XSDatabase()
        var dstPath = NSSearchPathForDirectoriesInDomains(.LibraryDirectory, .UserDomainMask, true).first!
        dstPath += "/Database.db"
        database.open(dstPath)
        return database
    }()
    
    /// 执行后，只得到一个整数结果
    func intForSelectSQL(sql: String) -> Int {
        var result: Int32 = 0
        selectSQL(sql) { (index, stmt) -> Void in
            result = sqlite3_column_int(stmt, 0)
        }
        return Int(result)
    }
    
    /// 用户版本
    var userVersion: Int {
        get {
            return intForSelectSQL("pragma user_version")
        }
        set {
            executeSQL("pragma user_version=\(newValue)")
        }
    }
}