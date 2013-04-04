
#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <fstream>
#include <cstring>
#include <iostream>

#include "../pf/pf.h"
#include "../rm/rm.h"

#define IX_EOF (-1)  // end of the index scan

const int fanout=340;

using namespace std;

//Error numbers
#define ECREATINDEX 1
#define EOPENINDEX 2
#define ECLOSEINDEX 3
#define EDESTROYINDEX 4
#define EINSERTENTRY 5
#define ESEARCHENT 6
#define EDELETEENTRY 7
#define EOPENSCAN 8
#define EGETNEXTENT 9
#define ECLOSESCAN 10



//node to store keys (made seperate to be reused by leafs as well)
template<class T>
struct node {
	int n;
	T keys[fanout];
	node() {
		for (int i = 0; i < fanout; i++) {
			keys[i] = -1;
		}
		n = 0;
	}
};

//intermediate for root and intermediate nodes
template<class T>
struct intermediate : node<T> {
	int ptrs[fanout+1];
	intermediate() {
		for (int i = 0; i < fanout+1; i++) {
			ptrs[i] = -1;
		}
	}
};

//leaf for leaf nodes, contains RID's
template<class T>
struct leaf: node<T> {
	RID rids[fanout];
	int forward;
	int backward; // do we really need this?
	leaf() {
		for (int i = 0; i < fanout; i++) {
			rids[i].pageNum = -1;
			rids[i].slotNum = -1;
		}
		forward = -1;
		backward = -1;
	}
};

struct Lineage {
	int page;
	int index;
};


class IX_IndexHandle;

class IX_Manager {
 PF_Manager *pf;
 public:
  static IX_Manager* Instance();

  RC CreateIndex(const string tableName,       // create new index
		 const string attributeName);
  RC DestroyIndex(const string tableName,      // destroy an index
		  const string attributeName);
  RC OpenIndex(const string tableName,         // open an index
	       const string attributeName,
	       IX_IndexHandle &indexHandle);
  RC CloseIndex(IX_IndexHandle &indexHandle);  // close index

 protected:
  IX_Manager   ();                             // Constructor
  ~IX_Manager  ();                             // Destructor

 private:
  static IX_Manager *_ix_manager;
};


class IX_IndexHandle {
 public:
  IX_IndexHandle  ();                           // Constructor
  ~IX_IndexHandle ();                           // Destructor

  PF_FileHandle fileHandle;
  string indexFileName;

  // The following two functions are using the following format for the passed key value.
  //  1) data is a concatenation of values of the attributes
  //  2) For int and real: use 4 bytes to store the value;
  //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
  RC InsertEntry(void *key, const RID &rid);  // Insert new index entry
  //rag
  template<class T>RC SearchEntry(T *reqKeyVal, CompOp cmpOp, int &pageNum, int &leafIndex, vector<Lineage> &keyLin);
  //endrag
  RC DeleteEntry(void *key, const RID &rid);  // Delete index entry
  template<class T>RC deleteKey(const vector<Lineage> &keyLin, int height, void *data, T k);
  template<class T>RC insertKey(int ptr,T k,const RID &rid,T &entry,int &page,int forward,int backward);
};

class IX_IndexScan {
public:
	PF_FileHandle fileHandle;
	int pageNum, leafIndex, noEntry;
	CompOp cmpOp;
	void *leafNodeBfr;
	bool eqScanFlag;

  IX_IndexScan();  								// Constructor
  ~IX_IndexScan(); 								// Destructor

  // for the format of "value", please see IX_IndexHandle::InsertEntry()
  RC OpenScan(const IX_IndexHandle &indexHandle, // Initialize index scan
	      CompOp      compOp,
	      void        *value);
  RC GetNextEntry(RID &rid);  // Get next matching entry
  RC CloseScan();             // Terminate index scan
  template<class T>
  RC FindLeafHeaderPage(int &pageNum, T *data);
};

// print out the error message for a given return code
void IX_PrintError (RC rc);


#endif
