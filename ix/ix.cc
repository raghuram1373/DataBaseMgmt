#include "ix.h"

PF_Manager *pf=PF_Manager::Instance();

IX_Manager *IX_Manager::_ix_manager = 0;
//IX_IndexHandle *IX_IndexHandle::_ix_indxHandle = 0;

IX_Manager *IX_Manager::Instance(){
	if(!_ix_manager)
		_ix_manager = new IX_Manager();
	return _ix_manager;
}

IX_Manager::IX_Manager(){
}

IX_Manager::~IX_Manager(){
}

IX_IndexHandle::IX_IndexHandle(){
}

IX_IndexHandle::~IX_IndexHandle(){
}

IX_IndexScan::IX_IndexScan(){
}

IX_IndexScan::~IX_IndexScan(){
}

template<class T>
RC IX_IndexScan::FindLeafHeaderPage(int &pageNum, T *data){
	int nodeIndx = 0;

	fileHandle.ReadPage(pageNum, data);
	if(*(int *)data != 0){
		return 0;
	}
	else {
		while(nodeIndx < fanout){
			if((pageNum = ((intermediate<T> *) (data+sizeof(int)))->ptrs[nodeIndx]) != -1){
				return FindLeafHeaderPage<T>(pageNum, data);
			}
		}
	}
	return -1;
}

RC IX_IndexScan::OpenScan(const IX_IndexHandle &indexHandle, CompOp compOp, void *value){
	vector<Lineage> keyLin;
	IX_IndexHandle ixHandle;
	int rc = pf->OpenFile(indexHandle.indexFileName.c_str(), fileHandle);
	if(rc == -1)
		return -1;
	pf->OpenFile(indexHandle.indexFileName.c_str(), ixHandle.fileHandle);
	void *bookData = malloc(PF_PAGE_SIZE);
	fileHandle.ReadPage(0, bookData);
	AttrType attrType;
	memcpy(&attrType, (char *)bookData+4, sizeof(attrType));

	cmpOp = compOp;

	if(cmpOp != NO_OP){
		if (attrType == TypeInt) {
			rc = ixHandle.SearchEntry((int *) value, cmpOp, pageNum, leafIndex, keyLin);
		} else if (attrType == TypeReal) {
			rc = ixHandle.SearchEntry((float *) value, cmpOp, pageNum, leafIndex, keyLin);
		}
		if(rc == -1){
			if(cmpOp == EQ_OP)
				eqScanFlag = true;
			else if(cmpOp == LT_OP || cmpOp == LE_OP)
				leafIndex--;
		}
		else if(cmpOp == EQ_OP){
			eqScanFlag = false;
		}
	}
	else {
		void *data = malloc(PF_PAGE_SIZE);
		pageNum = 1;
		if (attrType == TypeInt) {
			FindLeafHeaderPage(pageNum, (int *)data);
		} else if (attrType == TypeReal) {
			FindLeafHeaderPage(pageNum, (float *)data);
		}
		leafIndex = 0;
		free(data);
	}
	free(bookData);
	leafNodeBfr = malloc(PF_PAGE_SIZE);
	noEntry = fileHandle.ReadPage(pageNum, leafNodeBfr);
	pf->CloseFile(ixHandle.fileHandle);
	return 0;
}

template<class T>
RC IX_IndexHandle::SearchEntry(T *reqKeyPtr, CompOp cmpOp, int &pageNum, int &leafIndex, vector<Lineage> &keyLin){
	int nodeIndx, parentPage;
	T keyValue, leafKey;
	void *nodeBfr = malloc(PF_PAGE_SIZE);
	intermediate<T> nextNode;

	void *tmpData = malloc(PF_PAGE_SIZE);
	fileHandle.ReadPage(0, tmpData);
	int rootPage = 0;
	memcpy(&rootPage, (char *)tmpData, sizeof(int));
	free(tmpData);
	fileHandle.ReadPage(rootPage, nodeBfr);
	pageNum = rootPage;
	while(true){
		parentPage = pageNum;
		nodeIndx = 0;
		intermediate<T> N;
		memcpy(&N, (char *)nodeBfr+4, sizeof(N));
		/*for (int i = 0; i < fanout ; i++) {
			cout << N.keys[i] << endl;
		}*/
		while(nodeIndx<fanout){
			keyValue = N.keys[nodeIndx];
			if(N.ptrs[nodeIndx] != -1){
				pageNum = N.ptrs[nodeIndx];
			}
			if(keyValue != -1 && keyValue <= *reqKeyPtr) {
				pageNum = N.ptrs[nodeIndx+1];
 				break;
			}
			nodeIndx++;
		}

		if(nodeIndx == fanout)
			pageNum = (((struct intermediate<T> *) ((char *)nodeBfr+sizeof(int)))->ptrs[nodeIndx]);

		if(pageNum == -1)
			return -1;
		//talk to chandu
		Lineage lin;
		lin.index = nodeIndx;
		lin.page = parentPage;
		keyLin.push_back(lin);
		fileHandle.ReadPage(pageNum, nodeBfr);
		if(*(int *)nodeBfr == 0){
			//nothing
		}
		else {
			leafIndex = 0;
			while(leafIndex < fanout){
				leafKey = ((struct leaf<T> *) ((char *)nodeBfr+sizeof(int)))->keys[leafIndex];
				if(leafKey == *reqKeyPtr){
					if(cmpOp == GT_OP){
						if(leafIndex < fanout-1)
							leafIndex++;
						else {
							pageNum = ((struct leaf<T> *) ((char *)nodeBfr+sizeof(int)))->forward;
							leafIndex = 0;
						}
					}
					else if(cmpOp == LT_OP){
						if(leafIndex > 0)
							leafIndex--;
						else {
							pageNum = ((struct leaf<T> *) ((char *)nodeBfr+sizeof(int)))->backward;
							leafIndex = 0;
						}
					}
					return 0;
				}
				else if(leafKey > *reqKeyPtr)
					break;
				leafIndex++;
			}
			return -1;
		}
	}
}

RC IX_IndexScan::GetNextEntry(RID &rid){
	int keyVal;

	if(noEntry == -1)
		return -1;
	void *bookData = malloc(PF_PAGE_SIZE);
	fileHandle.ReadPage(0, bookData);
	AttrType attrType;
	memcpy(&attrType, (char *)bookData+4, sizeof(attrType));

	switch (cmpOp) {
	case EQ_OP:
		if(eqScanFlag)
			return IX_EOF;
		if (attrType == TypeInt){
			keyVal = ((struct leaf<int> *) ((char *)leafNodeBfr + sizeof(int)))->keys[leafIndex];
			rid = ((struct leaf<int> *) ((char *)leafNodeBfr + sizeof(int)))->rids[leafIndex];
		}
		else if (attrType == TypeReal){
			keyVal = ((struct leaf<float> *) ((char *)leafNodeBfr + sizeof(int)))->keys[leafIndex];
			rid = ((struct leaf<float> *) ((char *)leafNodeBfr + sizeof(int)))->rids[leafIndex];
		}
		eqScanFlag = true;
		break;
	case LT_OP:      // <
	case LE_OP:      // <=
		if(leafIndex == -1){
			if (attrType == TypeInt){
				pageNum = ((struct leaf<int> *) ((char *)leafNodeBfr + sizeof(int)))->backward;
			}
			else if (attrType == TypeReal){
				pageNum = ((struct leaf<float> *) ((char *)leafNodeBfr + sizeof(int)))->backward;
			}
			if(pageNum != -1){
				fileHandle.ReadPage(pageNum, leafNodeBfr);
				leafIndex = fanout-1;
			}
			else
				return IX_EOF;
		}
		if (attrType == TypeInt){
			keyVal = ((struct leaf<int> *) ((char *)leafNodeBfr + sizeof(int)))->keys[leafIndex];
		}
		else if (attrType == TypeReal){
			keyVal = ((struct leaf<float> *) ((char *)leafNodeBfr + sizeof(int)))->keys[leafIndex];
		}
		if(keyVal != -1){
			if (attrType == TypeInt){
				rid = ((struct leaf<int> *) ((char *)leafNodeBfr + sizeof(int)))->rids[leafIndex];
			}
			else if (attrType == TypeReal){
				rid = ((struct leaf<float> *) ((char *)leafNodeBfr + sizeof(int)))->rids[leafIndex];
			}
		}
		else {
			leafIndex--;
			return GetNextEntry(rid);
		}
		leafIndex--;
		break;
	case GT_OP:      // >
	case GE_OP:      // >=
	case NO_OP:
		if(leafIndex == fanout){
			if (attrType == TypeInt){
				pageNum = ((struct leaf<int> *) ((char *)leafNodeBfr + sizeof(int)))->forward;
			}
			else if (attrType == TypeReal){
				pageNum = ((struct leaf<float> *) ((char *)leafNodeBfr + sizeof(int)))->forward;
			}
			if(pageNum != -1){
				fileHandle.ReadPage(pageNum, leafNodeBfr);
				leafIndex = 0;
			}
			else
				return IX_EOF;
		}
		if (attrType == TypeInt){
			keyVal = ((struct leaf<int> *) ((char *)leafNodeBfr + sizeof(int)))->keys[leafIndex];
		}
		else if (attrType == TypeReal){
			keyVal = ((struct leaf<float> *) ((char *)leafNodeBfr + sizeof(int)))->keys[leafIndex];
		}
		if(keyVal != -1){
			if (attrType == TypeInt){
				rid = ((struct leaf<int> *) ((char *)leafNodeBfr + sizeof(int)))->rids[leafIndex];
			}
			else if (attrType == TypeReal){
				rid = ((struct leaf<float> *) ((char *)leafNodeBfr + sizeof(int)))->rids[leafIndex];
			}
		}
		else {
 	 	 	leafIndex++;
			return GetNextEntry(rid);
		}
		leafIndex++;
		break;
	}
	return 0;
}

RC IX_IndexScan::CloseScan(void){
	pageNum = -1;
	leafIndex = 0;
	pf->CloseFile(fileHandle);
	free(leafNodeBfr);
	return 0;
}

RC IX_IndexHandle::DeleteEntry(void *key, const RID &rid){
	int pageNum, leafIndex, result, height;
	void *data = malloc(PF_PAGE_SIZE);
	vector<Lineage> keyLin;
	void *bookData = malloc(PF_PAGE_SIZE);
	fileHandle.ReadPage(0, bookData);
	AttrType attrType;
	memcpy(&attrType, (char *)bookData+4, sizeof(attrType));
	if (attrType == TypeInt) {
		result = SearchEntry((int *)key, EQ_OP, pageNum, leafIndex, keyLin);
	} else if (attrType == TypeReal) {
		result = SearchEntry((float *)key, EQ_OP, pageNum, leafIndex, keyLin);
	}
	if(result != 0)
		return result;
	Lineage lin;
	lin.index = leafIndex;
	lin.page = pageNum;
	keyLin.push_back(lin);
	height = keyLin.size();

	if (attrType == TypeInt) {
		int test = 1;
		result = deleteKey(keyLin, height-1, data, test);
	} else if (attrType == TypeReal) {
		float test = 1;
		result = deleteKey(keyLin, height-1, data, test);
	}
	free(data);
	free(bookData);
	return result;
}

template<class T>
RC IX_IndexHandle::deleteKey(const vector<Lineage> &keyLin, int height, void *data, T test){
	fileHandle.ReadPage(keyLin[height].page, data);

	if(*(int *)data == 0){
		((intermediate<T> *) ((char *)data+sizeof(int)))->keys[keyLin[height].index] = -1;
		//if(keyLin[height].index != 0)
		((intermediate<T> *) ((char *)data+sizeof(int)))->ptrs[keyLin[height].index+1] = -1;
		((intermediate<T> *) ((char *)data+sizeof(int)))->n -= 1;

		fileHandle.WritePage(keyLin[height].page, data);
		if(((intermediate<T> *) ((char *)data+sizeof(int)))->n == 0 && ((intermediate<T> *) ((char *)data+sizeof(int)))->ptrs[0] == -1){
			if(height == 0)
				return 0;
			else
				return deleteKey<T>(keyLin, height-1, data, test);
		}
		else
			return 0;
	}
	else {
		((leaf<T> *) ((char *)data+sizeof(int)))->keys[keyLin[height].index] = -1;
		((leaf<T> *) ((char *)data+sizeof(int)))->n -= 1;
		fileHandle.WritePage(keyLin[height].page, data);
		if(((leaf<T> *) ((char *)data+sizeof(int)))->n == 0){
			if(((leaf<T> *) ((char *)data+sizeof(int)))->backward != -1){
				void *bData = malloc(PF_PAGE_SIZE);
				fileHandle.ReadPage(((leaf<T> *) ((char *)data+sizeof(int)))->backward, bData);
				((leaf<T> *) ((char *)bData+sizeof(int)))->forward = ((leaf<T> *) ((char *)data+sizeof(int)))->forward;
				fileHandle.WritePage(((leaf<T> *) ((char *)data+sizeof(int)))->backward, bData);
				free(bData);
			}
			if(((leaf<T> *) ((char *)data+sizeof(int)))->forward != -1){
				void *fData = malloc(PF_PAGE_SIZE);
				fileHandle.ReadPage(((leaf<T> *) ((char *)data+sizeof(int)))->forward, fData);
				((leaf<T> *) ((char *)fData+sizeof(int)))->backward = ((leaf<T> *) ((char *)data+sizeof(int)))->backward;
				fileHandle.WritePage(((leaf<T> *) ((char *)data+sizeof(int)))->forward, fData);
				free(fData);
			}
			return deleteKey<T>(keyLin, height-1, data, test);
		}
		else
			return 0;
	}
	return 0;
}

RC IX_Manager::CloseIndex(IX_IndexHandle &indexHandle){
	int rc;
	rc = pf->CloseFile(indexHandle.fileHandle);
	return rc;
}

RC IX_Manager::DestroyIndex(const string tableName, const string attributeName){
	int rc;
	rc = pf->DestroyFile((tableName+"."+attributeName).c_str());
	return rc;
}

void IX_PrintError (RC rc){
	switch (rc) {
	case ECREATINDEX:
		cout << "Failed to create Index file" << endl;
		break;
	case EOPENINDEX:
		cout << "Failed to open Index file"<< endl;
		break;
	case ECLOSEINDEX:
		cout << "Failed to close Index file" << endl;
		break;
	case EDESTROYINDEX:
		cout << "Failed to destroy Index file" << endl;
		break;
	case EINSERTENTRY:
		cout << "Failed to insert Index entry" << endl;
		break;
	case ESEARCHENT:
		cout << "Failed to search Index entry" << endl;
		break;
	case EDELETEENTRY:
		cout << "Failed to delete Index entry" << endl;
		break;
	case EOPENSCAN:
		cout << "Failed to open Index file for scan operation" << endl;
		break;
	case EGETNEXTENT:
		cout << "Failed to get next entry of Index file in scan operation" << endl;
		break;
	case ECLOSESCAN:
		cout << "Failed to close Index file after scan" << endl;
		break;
	}
}

//creates the index file and does some preprocessing.
RC IX_Manager::CreateIndex(const string tableName, const string attributeName) {
	RM *rm = RM::Instance();
	string indexFileName = tableName+"."+attributeName;
	int rc = pf->CreateFile(indexFileName.c_str());
	if (rc == -1) return rc;
	PF_FileHandle fileHandle;
	rc = pf->OpenFile(indexFileName.c_str(), fileHandle);
	if (rc == -1) return rc;
	int rootFlag = 0, offset = 0;
	void *data = malloc(PF_PAGE_SIZE);
	memcpy((char *)data, &rootFlag, sizeof(int));
	offset += sizeof(int);
	vector<Attribute> attrs;
	rc = rm->getAttributes(tableName, attrs);
	for (int i = 0 ; i < (int)attrs.size(); i++) {
		if (attrs[i].name == attributeName) {
			memcpy((char*)data+offset,&(attrs[i].type),sizeof(attrs[i].type));
			offset += sizeof(attrs[i].type);
			break;
		}
	}

	//add fanout
	memcpy((char *)data+offset, &fanout, sizeof(int));
	rc = fileHandle.AppendPage(data);
	if (rc == -1) return rc;
	rc = pf->CloseFile(fileHandle);
	if (rc == -1) return rc;
	free(data);
	return rc;
}

RC IX_Manager::OpenIndex(const string tableName, const string attributeName, IX_IndexHandle &indexHandle) {
	int rc = -1;
	indexHandle.indexFileName = tableName + "." + attributeName;

	rc = pf->OpenFile((indexHandle.indexFileName).c_str(), indexHandle.fileHandle);
	return rc;
}

RC IX_IndexHandle::InsertEntry(void *key, const RID &rid) {
	//ckeck if open?
	if (!fileHandle.stream.is_open()) return -1;
	void *data = malloc(PF_PAGE_SIZE);
	fileHandle.ReadPage(0, data);

	int x = 0, offset = 0, rc = -1, page = -1;
	memcpy(&x, (char *)data, sizeof(int));
	offset += sizeof(int);

	AttrType attrType;
	memcpy(&attrType, (char *)data+offset, sizeof(attrType));
	int forward=-1,backward=-1;
	if (attrType == TypeInt) {
		int k = -1, entry = -1;
		k = *(int *)key;
		rc = insertKey(x,k,rid,entry,page,forward,backward);
	} else if (attrType == TypeReal) {
		float k = -1, entry = -1;
		k = *(float *)key;
		rc = insertKey(x,k,rid,entry,page,forward,backward);
	} else {
		cout << "Strings and other data types not supported yet" << endl;
	}
	free(data);
	if (rc == 1) return rc;
	return 0;
}

//recursive method handling inserts and splits.
template<class T>
RC IX_IndexHandle::insertKey(int ptr,T k,const RID &rid,T &entry,int &page,int forward,int backward) {
	int flag;
	int exists = 0;

	if(ptr < -1)
	{
		leaf<T> L;

		L.keys[0] = k;
		L.rids[0] = rid;

		L.n++;

		L.forward = forward;
		L.backward = backward;

		void *data = malloc(PF_PAGE_SIZE);

		int x = 1;

		memcpy((char*)data,&x,sizeof(x));

		memcpy((char*)data+4,&L,sizeof(L));

		ptr = -(ptr);

		x = fileHandle.WritePage(ptr,data);

		page = ptr;

		if(forward!=-1)
		{
			x = fileHandle.ReadPage(forward,data);
			memcpy(&L,(char*)data+4,sizeof(L));
			L.backward = ptr;
			memcpy((char*)data+4,&L,sizeof(L));
			x = fileHandle.WritePage(forward,data);
		}

		if(backward!=-1)
		{
			x = fileHandle.ReadPage(backward,data);
			memcpy(&L,(char*)data+4,sizeof(L));
			L.forward = ptr;
			memcpy((char*)data+4,&L,sizeof(L));
			x = fileHandle.WritePage(backward,data);
		}
		free(data);
		return exists;
	}

	if(ptr == -1)
	{
		leaf<T> L;

		L.keys[0] = k;
		L.rids[0].pageNum = rid.pageNum;
		L.rids[0].slotNum = rid.slotNum;

		L.n++;
		L.forward = forward;
		L.backward = backward;
		void *data = malloc(PF_PAGE_SIZE);
		flag = 1;
		memcpy((char*)data,&flag,sizeof(int));
		memcpy((char*)data+4,&L,sizeof(L));
		int nop = fileHandle.GetNumberOfPages();
		int x = fileHandle.AppendPage(data);
		page = nop;
		if(forward!=-1)
		{
			x = fileHandle.ReadPage(forward,data);
			memcpy(&L,(char*)data+4,sizeof(L));
			L.backward = page;
			memcpy((char*)data+4,&L,sizeof(L));
			x = fileHandle.WritePage(forward,data);
		}

		if(backward!=-1)
		{
			x = fileHandle.ReadPage(backward,data);
			memcpy(&L,(char*)data+4,sizeof(L));
			L.forward = page;
			memcpy((char*)data+4,&L,sizeof(L));
			x = fileHandle.WritePage(backward,data);
		}
		free(data);
		return exists;
	}

	if(ptr == 0)
	{
		intermediate<T> N;
		N.keys[N.n] = k;
		N.n++;
		N.ptrs[1] = 2;
		leaf<T> L;
		L.keys[L.n] = k;
		L.rids[L.n] = rid;
		L.n++;
		void *data = malloc(PF_PAGE_SIZE);
		int flag = 0;
		memcpy((int*)data,&flag,sizeof(flag));
		memcpy((char*)data+4,&N,sizeof(N));
		fileHandle.AppendPage(data);
		flag = 1;
		memcpy((int*)data,&flag,sizeof(int));
		memcpy((char*)data+4,&L,sizeof(L));
		fileHandle.AppendPage(data);
		ptr = 1;
		void *tmpData = malloc(PF_PAGE_SIZE);
		int x = fileHandle.ReadPage(0,tmpData);
		memcpy((char*)tmpData,&ptr,sizeof(ptr));
		x = fileHandle.WritePage(0,tmpData);
		free(tmpData);
		free(data);
		return exists;
	}

	void *data = malloc(PF_PAGE_SIZE);
	int x = fileHandle.ReadPage(ptr,data);
	memcpy(&x,(int*)data,sizeof(int));
	int j;
	int root = 0;
	if(x == 0)
	{
		intermediate<T> N;
		memcpy(&N,(char*)data+4,sizeof(N));
		int i;
		for(i=0;i<N.n && N.keys[i]<=k;i++);
		int oldnodepointer = ptr;

		ptr = N.ptrs[i];

		if(i < fanout)
		{
			if(N.ptrs[i+1]!=-1)
			{
				forward = N.ptrs[i+1];
			}
			else if(forward!=-1)
			{
				void *data1 = malloc(PF_PAGE_SIZE);
				x = fileHandle.ReadPage(forward,data1);
				intermediate<T> tmp;
				memcpy(&tmp,(char*)data1+4,sizeof(tmp));
				forward = tmp.ptrs[0];
				free(data1);
			}
		}
		if(i > 0)
		{
			if(N.ptrs[i-1]!= -1)
			{
				backward = N.ptrs[i-1];
			} else if(backward!=-1) {
				void *data1 = malloc(PF_PAGE_SIZE);
				x = fileHandle.ReadPage(backward,data1);
				intermediate<T> tmp;
				memcpy(&tmp,(char*)data1+4,sizeof(tmp));
				backward = tmp.ptrs[tmp.n];
				free(data1);
			}
		}
		exists = insertKey(ptr,k,rid,entry,page,forward,backward);

		if(exists == 1)
		{
			free(data);
			return exists;
		}
		ptr = oldnodepointer;
		if(entry == -1)
		{
			if(page!=-1)
			{
				N.ptrs[i] = page;
				memcpy((char*)data+4,&N,sizeof(N));
				x = fileHandle.WritePage(ptr,data);
				page = -1;
			}
			free(data);
			return 0;
		}

		if(N.n < fanout)
		{
			for(j = N.n;j>i;j--)
			{
				N.keys[j] = N.keys[j-1];
				N.ptrs[j+1] = N.ptrs[j];
			}
			N.ptrs[j+1] = page;
			page = -1;
			N.keys[j] = entry;
			entry = -1;
			N.n++;
			memcpy((char*)data+4,&N,sizeof(N));
			x = fileHandle.WritePage(ptr,data);
			free(data);
			return 0;
		}
		intermediate<T> N2;
		T *keys;
		int *ptrs;
		keys = (T *)malloc((fanout+1)*sizeof(T));
		ptrs = (int *)malloc((fanout+2)*sizeof(int));
		for(int i=0;i<fanout+1;i++)
		{
			ptrs[i] = -1;
			keys[i] = -1;
		}
		ptrs[i] = -1;

		for(int i=0;i<fanout && N.keys[i]<=k;i++)
		{
			ptrs[i] = N.ptrs[i];
			keys[i] = N.keys[i];
		}
		ptrs[i] = N.ptrs[i];
		keys[i] = entry;
		i++;
		ptrs[i] = page;
		int d = fanout/2 ;
		for(;i<fanout+1;i++)
		{
			keys[i] = N.keys[i-1];
			ptrs[i+1] = N.ptrs[i];
		}

		intermediate<T> N1;
		for(i=0;i<d;i++)
		{
			N1.ptrs[i] = ptrs[i];
			N1.keys[i] = keys[i];
			N1.n++;
		}
		N1.ptrs[i] = ptrs[i];
		i++;

		for(i = d+1,j=0;i<fanout+1;i++,j++)
		{
			N2.ptrs[j] = ptrs[i];
			N2.keys[j] = keys[i];
			N2.n++;
		}

		N2.ptrs[j] = ptrs[i];
		memcpy((char*)data+4,&N1,sizeof(N1));
		x = fileHandle.WritePage(ptr,data);
		memcpy((char*)data+4,&N2,sizeof(N2));
		int nop = fileHandle.GetNumberOfPages();
		x = fileHandle.AppendPage(data);
		page = nop;
		entry = keys[d];
		x = fileHandle.ReadPage(0,data);
		int root;
		memcpy(&root,(int*)data,sizeof(int));
		if(root == ptr)
		{
			intermediate<T> N3;
			N3.keys[0] = keys[d];
			N3.ptrs[0] = ptr;
			N3.ptrs[1] = page;
			N3.n++;
			nop = fileHandle.GetNumberOfPages();
			flag = 0;
			memcpy((int*)data,&flag,sizeof(int));
			memcpy((char*)data+4,&N3,sizeof(N3));
			x = fileHandle.AppendPage(data);
			page = nop;
			void *tmpData = malloc(PF_PAGE_SIZE);
			int x1 = fileHandle.ReadPage(0,tmpData);
			memcpy((char*)tmpData,&ptr,sizeof(ptr));
			x1 = fileHandle.WritePage(0,tmpData);
			free(tmpData);
		}
		free(data);
		free(keys);
		free(ptrs);
		return 0;
	} else {
		leaf<T> L;
		int i;
		memcpy(&L,(char*)data+4,sizeof(L));
		for(i=0;i<L.n;i++)
		if(L.keys[i]==k && L.rids[i].pageNum==rid.pageNum && L.rids[i].slotNum == rid.slotNum)
		{
			free(data);
			return 1;
		}

		if(L.n<fanout)
		{
			for(i=0;i<L.n && L.keys[i]<=k;i++);
			if(i == L.n)
			{
				L.keys[L.n] = k;
				L.rids[L.n] = rid;
			} else {
				for(j=L.n;j>i;j--)
				{
					L.keys[j] = L.keys[j-1];
					L.rids[j] = L.rids[j-1];
				}
				L.keys[i] = k;
				L.rids[i] = rid;
			}
			L.n++;
			memcpy((char*)data+4,&L,sizeof(L));
			x = fileHandle.WritePage(ptr,data);
			entry = -1;
			free(data);
			return 0;
		} else {
			leaf<T> L2;
			T *keys = (T *)malloc((fanout+1)*sizeof(T));
			RID *rids = (RID *)malloc((fanout+1)*sizeof(RID));
			for(i=0;L.keys[i]<=k && i<L.n;i++)
			{
				keys[i] = L.keys[i];
				rids[i] = L.rids[i];
			}

			keys[i] = k;
			rids[i] = rid;
			i++;
			for(;i<fanout+1;i++)
			{
				keys[i] = L.keys[i-1];
				rids[i] = L.rids[i-1];
			}
			leaf<T> L1;
			L1.forward = L.forward;
			L1.backward = L.backward;
			int d = fanout/2;
			for(i=0;i<d;i++)
			{
				L1.keys[i] = keys[i];
				L1.rids[i] = rids[i];
				L1.n++;
			}

			for(j=0;i<fanout+1;i++,j++)
			{
				L2.keys[j] = keys[i];
				L2.rids[j] = rids[i];
				L2.n++;
			}

			int nop = fileHandle.GetNumberOfPages();

			L2.backward = ptr;
			L2.forward = L1.forward;
			L1.forward = nop;
			int y = 1;
			memcpy((char*)data,&y,sizeof(int));
			memcpy((char*)data+4,&L1,sizeof(L1));
			x = fileHandle.WritePage(ptr,data);
			memcpy((char*)data,&y,sizeof(int));
			memcpy((char*)data+4,&L2,sizeof(L2));
			x = fileHandle.AppendPage(data);
			page = nop;
			entry = L2.keys[0];
			leaf<T> L3;
			x = fileHandle.ReadPage(L2.forward,data);
			memcpy(&L3,(char*)data+4,sizeof(L3));
			L3.backward = L1.forward;
			memcpy((char*)data+4,&L3,sizeof(L3));
			x = fileHandle.WritePage(L2.forward,data);
			free(keys);
			free(rids);
			free(data);
			return 0;
		}
	}
}
