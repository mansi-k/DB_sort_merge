#include <iostream>
#include <sys/stat.h>
#include <vector>
#include <algorithm>
#include <bits/stdc++.h>
#include <chrono>

using namespace std;
using namespace std::chrono;


bool ascSortTable1(vector<string> const &r1, vector<string> const &r2) {
    if(r1[1] == r2[1])
        return (r1[0].compare(r2[0]))<0;
    return (r1[1].compare(r2[1]))<0;
}

bool ascSortTable2(vector<string> const &r1, vector<string> const &r2) {
    if(r1[0] == r2[0])
        return (r1[1].compare(r2[1]))<0;
    return (r1[0].compare(r2[0]))<0;
}

class AscRecCompareT1 {
public:
    bool operator()(vector<string> const &r1, vector<string> const &r2) {
        if(r1[1] == r2[1])
            return (r1[0].compare(r2[0]))>0;
        return (r1[1].compare(r2[1]))>0;
    }
};

class AscRecCompareT2 {
public:
    bool operator()(vector<string> const &r1, vector<string> const &r2) {
        if(r1[0] == r2[0])
            return (r1[1].compare(r2[1]))>0;
        return (r1[0].compare(r2[0]))>0;
    }
};



class TwoPhaseMergeSort {
private:
    vector<ifstream*> t1_openTempFilesVec;
    vector<ifstream*> t2_openTempFilesVec;
public:
    vector<int> sortColIndexVec;
    string table1File;
    string table2File;
    long int maxMemRows=0, recSize=0;
    string order;
    vector<string> sortColsVec;
    vector<pair<string,int>> metaVec;
    vector<vector<string>> recordsVec;
    vector<string> t1_tmpFilenamesVec;
    vector<string> t2_tmpFilenamesVec;
    int MBLOCKS = 0;
    int recsPerBlock = 2;
    bool *t1_completed_arr;
    bool *t2_completed_arr;
    vector<int> t1_num_recs_per_file_vec;
    vector<int> t2_num_recs_per_file_vec;
    vector<vector<string>> t2_curr_join_tuples;
    int lineSize;
//    priority_queue<vector<string>,vector<vector<string>>,T> PQ<T>;

    TwoPhaseMergeSort(string t1File, string t2File, int m_blocks) {
        table1File = t1File;
        table2File = t2File;
        MBLOCKS = m_blocks;
        maxMemRows = (long int) m_blocks*recsPerBlock;
        cout << "Memory size taken (in rows) : " << maxMemRows << endl;
    }

    void sortFile() {
        cout << "Running Phase 1" << endl;
        cout << "Phase-1 of Table-1" << endl;
        phaseOneSort(table1File,"T1");
        cout << "Phase-1 of Table-2" << endl;
        phaseOneSort(table2File,"T2");
//        exit(0);
        if(!t1_tmpFilenamesVec.empty() && !t2_tmpFilenamesVec.empty()) {
            if(t1_tmpFilenamesVec.size()+t2_tmpFilenamesVec.size() > MBLOCKS*MBLOCKS) {
                cout << "Memory constraint violated!" << endl;
                exit(0);
            }
            cout << "Running Phase 2" << endl;
            phaseTwoSort();
        }
    }

    void phaseOneSort(string tableFile, string curT) {
        struct stat filestatus;
        stat(tableFile.c_str(), &filestatus);
        long int filesize = filestatus.st_size;
//        cout << "Filesize : " << filesize << endl;
        long int recs_toread = (long int) maxMemRows;
//        cout << "Number of sub-files (splits) : " << (long int)filesize/(recs_toread*recSize) << endl;
        cout << "Records to read for a sub-file : " << recs_toread << endl;
        ifstream datafile(tableFile);
        string line;
        long int cur_line_count=0, all_recs_count=0;
        int sublist_num=0;
        while(cur_line_count < recs_toread && getline(datafile, line)) {
            lineSize = line.length();
            recordsVec.push_back(recToVec(line));
            cur_line_count++;
            if(cur_line_count == recs_toread) {
                cout << "Sorting sublist #" << sublist_num << endl;
                if(curT == "T1")
                    sort(recordsVec.begin(),recordsVec.end(),ascSortTable1);
                else
                    sort(recordsVec.begin(),recordsVec.end(),ascSortTable2);
                writeTempFile(sublist_num,curT);
                cur_line_count = 0;
                sublist_num++;
                recordsVec.clear();
            }
            all_recs_count++;
        }
        if(recordsVec.size()>0) {
            cout << "Sorting sublist #" << sublist_num << endl;
            if(curT == "T1")
                sort(recordsVec.begin(),recordsVec.end(),ascSortTable1);
            else
                sort(recordsVec.begin(),recordsVec.end(),ascSortTable2);
            writeTempFile(sublist_num,curT);
            cur_line_count = 0;
            sublist_num++;
            recordsVec.clear();
        }
//        cout << "Records sorted : " << all_recs_count << endl;
    }

    void writeTempFile(int sublist_num, string curT) {
        cout << "Writing to disk #" << sublist_num << endl;
        string tmp_filename = curT+"_sublist"+to_string(sublist_num)+".txt";
        ofstream tmpfile;
        tmpfile.open(tmp_filename.c_str(), ios_base::app);
        if(!tmpfile.is_open()) {
            cout << "not open " << tmp_filename << endl;
        }
//        cout << "is open " << tmp_filename << " " << recordsVec.size() << endl;
        for(int i=0; i<recordsVec.size(); i++) {
            string recline = vecToStr(recordsVec[i], false);
//            cout << recline << endl;
            if (i == recordsVec.size() - 1)
                tmpfile << recline;
            else
                tmpfile << recline << "\n";
        }
        tmpfile.close();
        if(curT == "T1") {
            t1_tmpFilenamesVec.push_back(tmp_filename);
            t1_num_recs_per_file_vec.push_back(recordsVec.size());
        }
        else {
            t2_tmpFilenamesVec.push_back(tmp_filename);
            t2_num_recs_per_file_vec.push_back(recordsVec.size());
        }
    }

    void phaseTwoSort() {
        openTempFiles();
//        cout << "Temp files opened" << endl;
        ofstream *outFile = new ofstream("T1_T2_join", ios::out | ios::app);
        int t1_nof_subfiles = t1_tmpFilenamesVec.size();
        int t2_nof_subfiles = t2_tmpFilenamesVec.size();
//        int block_size = (int)maxMemSize/(sublist_num*recSize);
        int block_size = recsPerBlock;
        string line;
        bool t1_is_completed[t1_nof_subfiles];
        bool t2_is_completed[t2_nof_subfiles];
        t1_completed_arr = t1_is_completed;
        t2_completed_arr = t2_is_completed;
        int t1_subfile_access_arr[t1_nof_subfiles];
        int t2_subfile_access_arr[t2_nof_subfiles];
        priority_queue<vector<string>,vector<vector<string>>,AscRecCompareT1> t1_PQ;
        priority_queue<vector<string>,vector<vector<string>>,AscRecCompareT2> t2_PQ;
        for(int i=0;i<t1_nof_subfiles;i++) {
            t1_subfile_access_arr[i] = 0;
            for(vector<string> const rvec : readDataBlock(i, t1_openTempFilesVec[i], block_size, "T1"))
                t1_PQ.push(rvec);
        }
        cout << "1st block from all T1 subfiles read" << endl;
        for(int i=0;i<t2_nof_subfiles;i++) {
            t2_subfile_access_arr[i] = 0;
            for(vector<string> const rvec : readDataBlock(i, t2_openTempFilesVec[i], block_size, "T2"))
                t2_PQ.push(rvec);
        }
        cout << "Sorting ..." << endl;
        while(!t1_PQ.empty() && !t2_PQ.empty()) {
            int last_col_idx = 2;
//            vector<vector<string>> t2_curr_join_tuples;
            vector<string> t1_top_record = t1_PQ.top();
            vector<string> t2_top_record = t2_PQ.top();
            cout << "T1 PQ size " << t1_PQ.size() << " " << t1_top_record[0] << " " << t1_top_record[1] << endl;
            cout << "T2 PQ size" << t2_PQ.size() << " " << t2_top_record[0] << " " << t2_top_record[1] << endl;
//            t1_PQ.pop();
//            t2_PQ.pop();
            int t1_top_subfile_num = stoi(t1_top_record[last_col_idx]);
            int t2_top_subfile_num = stoi(t2_top_record[last_col_idx]);
//            cout << "TOP " << top_block_num << " " << top_record[2] << endl;
            t1_subfile_access_arr[t1_top_subfile_num]++;
            t2_subfile_access_arr[t2_top_subfile_num]++;
//            cout << top_block_num << "access" << block_access_arr[top_block_num] << endl;
            if(t1_top_record[1].compare(t2_top_record[0]) > 0) {
                // get next tuple from t2
                cout << "T1 > T2" << endl;
                t2_PQ.pop();
                if(t2_subfile_access_arr[t2_top_subfile_num] >= block_size && !t2_completed_arr[t2_top_subfile_num]) {
                    for(vector<string> const rvec : readDataBlock(t2_top_subfile_num, t2_openTempFilesVec[t2_top_subfile_num], block_size, "T2"))
                        t2_PQ.push(rvec);
                    t2_subfile_access_arr[t2_top_subfile_num] = 0;
                }
            }
            else if(t1_top_record[1].compare(t2_top_record[0]) < 0) {
                // get next tuple from t1
                cout << "T1 < T2" << endl;
                t1_PQ.pop();
                if(t1_subfile_access_arr[t1_top_subfile_num] >= block_size && !t1_completed_arr[t1_top_subfile_num]) {
                    for(vector<string> const rvec : readDataBlock(t1_top_subfile_num, t1_openTempFilesVec[t1_top_subfile_num], block_size, "T1"))
                        t1_PQ.push(rvec);
                    t1_subfile_access_arr[t1_top_subfile_num] = 0;
                }
            }
            else {
                cout << "T1 = T2" << endl;
                t2_top_record.pop_back(); // remove subfile number from end
                t2_curr_join_tuples.push_back(t2_top_record);
                t2_PQ.pop();
                if(t2_subfile_access_arr[t2_top_subfile_num] >= block_size && !t2_completed_arr[t2_top_subfile_num]) {
                    for(vector<string> const rvec : readDataBlock(t2_top_subfile_num, t2_openTempFilesVec[t2_top_subfile_num], block_size, "T2"))
                        t2_PQ.push(rvec);
                    t2_subfile_access_arr[t2_top_subfile_num] = 0;
                }
                while(!t2_PQ.empty() && t1_top_record[1]==t2_PQ.top()[0]) {
                    t2_top_record = t2_PQ.top();
                    t2_PQ.pop();
                    t2_top_subfile_num = stoi(t2_top_record[last_col_idx]);
                    t2_subfile_access_arr[t2_top_subfile_num]++;
                    t2_top_record.pop_back();
                    t2_curr_join_tuples.push_back(t2_top_record);
                    if(t2_subfile_access_arr[t2_top_subfile_num] >= block_size && !t2_completed_arr[t2_top_subfile_num]) {
                        for(vector<string> const rvec : readDataBlock(t2_top_subfile_num, t2_openTempFilesVec[t2_top_subfile_num], block_size, "T2"))
                            t2_PQ.push(rvec);
                        t2_subfile_access_arr[t2_top_subfile_num] = 0;
                    }
                }
                store_by_join(outFile, t1_top_record);
                t1_PQ.pop();
                if(t1_subfile_access_arr[t1_top_subfile_num] >= block_size && !t1_completed_arr[t1_top_subfile_num]) {
                    for(vector<string> const rvec : readDataBlock(t1_top_subfile_num, t1_openTempFilesVec[t1_top_subfile_num], block_size, "T1"))
                        t1_PQ.push(rvec);
                    t1_subfile_access_arr[t1_top_subfile_num] = 0;
                }
                while(!t1_PQ.empty() && t1_PQ.top()[1]==t2_top_record[0]) {
                    t1_top_record = t1_PQ.top();
                    t1_PQ.pop();
                    t1_top_subfile_num = stoi(t1_top_record[last_col_idx]);
                    t1_subfile_access_arr[t1_top_subfile_num]++;
                    store_by_join(outFile, t1_top_record);
                    if(t1_subfile_access_arr[t1_top_subfile_num] >= block_size && !t1_completed_arr[t1_top_subfile_num]) {
                        for(vector<string> const rvec : readDataBlock(t1_top_subfile_num, t1_openTempFilesVec[t1_top_subfile_num], block_size, "T1"))
                            t1_PQ.push(rvec);
                        t1_subfile_access_arr[t1_top_subfile_num] = 0;
                    }
                }
                t2_curr_join_tuples.clear();
            }
        }
        outFile->close();
    }

    vector<vector<string>> readDataBlock(int filenum, ifstream* subfile, int block_size, string curT) {
        string line, word;
        vector<vector<string>> block_recs;
        for(int b=0;b<block_size;b++) {
            cout << "Getting nextline from subfile " << filenum << endl;
            if(subfile->peek() && getline(*subfile,line)) {
//                cout << "in if of get" << endl;
                cout << line << endl;
                vector<string> record = recToVec(line);
//                cout << "after rectovec" << endl;
                record.push_back(to_string(filenum));
                block_recs.push_back(record);

                if(curT == "T1") {
                    t1_num_recs_per_file_vec[filenum]--;
                    if (t1_num_recs_per_file_vec[filenum] <= 0) {
                        t1_completed_arr[filenum] = true;
                        break;
                    }
                }
                else {
                    t2_num_recs_per_file_vec[filenum]--;
                    if (t2_num_recs_per_file_vec[filenum] <= 0) {
                        t2_completed_arr[filenum] = true;
                        break;
                    }
                }
            }
            else {
                cout << "error reading red" << endl;
                break;
            }
//            cout << "read " << filenum << endl;
        }
        return block_recs;
    }

    void store_by_join(ofstream* outFile, vector<string> t1_tuple) {
        for(vector<string> const t2_tuple : t2_curr_join_tuples) {
            string joined_tuple = t1_tuple[0] + " " + t1_tuple[1] + " " + t2_tuple[1];
            *outFile << joined_tuple << "\n";
        }
    }

    vector<string> recToVec(string line) {
        vector<string> record;
        int delim = line.find(' ');
        record.push_back(line.substr(0,delim));
        record.push_back(line.substr(delim+1));
        return record;
    }

    string vecToStr(vector<string> &rec, bool cutlast) {
        int n = cutlast ? rec.size()-1 : rec.size();
        string line="";
        for(int i=0;i<n;i++) {
            if(i == n-1)
                line += rec[i];
            else
                line += rec[i]+" ";
        }
        return line;
    }

    void openTempFiles() {
        for (int i=0;i<t1_tmpFilenamesVec.size();i++) {
            t1_openTempFilesVec.push_back(new ifstream(t1_tmpFilenamesVec[i].c_str(), ios::in));
        }
        for (int i=0;i<t2_tmpFilenamesVec.size();i++) {
            t2_openTempFilesVec.push_back(new ifstream(t2_tmpFilenamesVec[i].c_str(), ios::in));
        }
    }

    void closeTempFiles() {
        for (int i=0;i<t1_openTempFilesVec.size();i++) {
            t1_openTempFilesVec[i]->close();
            remove(t1_tmpFilenamesVec[i].c_str());
        }
        for (int i=0;i<t2_openTempFilesVec.size();i++) {
            t2_openTempFilesVec[i]->close();
            remove(t2_tmpFilenamesVec[i].c_str());
        }
    }

};

int main(int argc, char ** argv) {
    if(argc<4) {
        cout << "Usage : main.cpp <table1_file> <table2_file> <M_blocks>" << endl;
        return 0;
    }
//    cout << endl;
    TwoPhaseMergeSort *tpms = new TwoPhaseMergeSort(argv[1],argv[2],stoi(argv[3]));
    cout << "Started Execution" << endl;
    auto ex_start = high_resolution_clock::now();
    tpms->sortFile();
    auto ex_stop = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(ex_stop - ex_start);
    cout << "Time taken for joining : " << ((double)duration.count()/1000) << " seconds" << endl;
    tpms->closeTempFiles();
    return 0;
}