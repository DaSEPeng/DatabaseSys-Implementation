/*
 * 实验1：练习levelDB的快照与迭代器的使用
 * 姓名: 李鹏
 * 日期：2020年3月23日
 *
 * 注意：
 *  - 要首先要将这个文件的路径添加到leveldb源码下面的CMakeLists.txt文件
 *  - 文件的路径是相对于leveldb的根目录的，而不是当前文件所在目录
 */


#include "leveldb/db.h"
#include <iostream>       // std::cout
#include <fstream>        // std::ifstream
#include <string>         // std::string
#include <cstdlib>        // std::exit()


// 将文件中的成绩信息写入数据库，注意结尾不是冒号，不是python...
int write2db(std::string input_path, leveldb::DB* tmp_db) { 
    // 几个基础变量
    std::string tmp_stuID;
    std::string tmp_line;
    std::string tmp_stuScore_str;
    int tmp_stuScore;

    // 读取input文件，注意最后一行的结尾没有换行符
    std::ifstream infile(input_path.c_str());
    assert(infile.is_open());

    // 读取input文件，注意最后一行的结尾没有换行符
    while (std::getline(infile,tmp_line)) // line中不包括每行的换行符
    {
        tmp_stuID = tmp_line.substr(0,3);
        tmp_stuScore = std::atoi(tmp_line.substr(4).c_str());
        tmp_stuScore_str = std::to_string(tmp_stuScore);
        leveldb::Status s = tmp_db->Put(leveldb::WriteOptions(),tmp_stuID,tmp_stuScore_str);
        assert(s.ok());
    }

    // 关闭文件
    infile.close();  
    return 0;
    }

int main() {
    // 输入输出文件地址
    std::string input1_path = "test/lab1/input1";
    std::string input2_path = "test/lab1/input2";
    std::string output_path = "test/lab1/output";

    // 创建数据库
    leveldb::DB* db = nullptr;
    leveldb::Options op;
    op.create_if_missing = true;
    leveldb::Status s = leveldb::DB::Open(op,"lab1_db", &db);
    assert(s.ok());

    // 将第一次成绩写入数据库
    write2db(input1_path,db);

    // 创建快照
    leveldb::ReadOptions snap_op;
    snap_op.snapshot = db->GetSnapshot();       

    // 将第二次成绩写入数据库
    write2db(input2_path,db);

    // 合并两次成绩，输出output(从头写)，为了方便并没有单独读取快照和数据库，而是同时读取的
    std::ofstream outfile(output_path.c_str());
   
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    leveldb::Iterator* snap_it = db->NewIterator(snap_op);
    it->SeekToFirst();
    snap_it->SeekToFirst();

    while(it->Valid() && snap_it->Valid())
    {
        outfile << it->key().ToString() << " " << it->value().ToString() << " " << snap_it->value().ToString() << std::endl;
        it->Next();
        snap_it->Next(); 
    }
    assert(it->status().ok() && snap_it->status().ok()); 

    std::cout << "Bingo!" << std::endl;
    delete it;
    delete snap_it;

    // 释放资源
    db->ReleaseSnapshot(snap_op.snapshot);
    outfile.close();
    delete db;
    return 0;
}
