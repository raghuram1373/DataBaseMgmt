
#include "rm.h"
#include<string.h>
#include<stdlib.h>
#include<malloc.h>
#include<iostream>

#define byteArraySize 16
#define slotSize sizeof(Slot)


RM* RM::_rm = 0;

RM* RM::Instance()
{
    if(!_rm)
        _rm = new RM();
    
    return _rm;
}

RM::RM()
{
	pf = PF_Manager::Instance();
	createCatalog();
	tableCount = 2; //tables and columns i.e., catalog info
}

RM::~RM()
{
}



void RM::createCatalog() {
	//creates a catalog
	int rc = pf->CreateFile("Tables");
	if (rc == -1) {
		return;
	}
	rc = pf->CreateFile("Columns");
	if (rc == -1) {
		return;
	}
	int counter = 0; //keep track of data in tables
	int counter1 = 0; // keep track of data in columns
	int tid1 = 1; //tid for tables
	int tid2 = 2; //tid for columns

	//RID for the record
	RID rid;

	//start putting data in Tables files as tuples
	void *tableData = malloc(100);
	memcpy((char *)tableData+counter, &tid1, sizeof(int));
	counter += sizeof(int);

	int sizeOfTablesChar = 6;
	char tables[] = "Tables";

	//Add table name
	memcpy((char *)tableData+counter, &sizeOfTablesChar, sizeof(int));
	counter += sizeof(int);

	memcpy((char *)tableData+counter, tables, sizeOfTablesChar);
	counter += sizeOfTablesChar;

	//do the same thing again for filename
	memcpy((char *)tableData+counter, &sizeOfTablesChar, sizeof(int));
	counter += sizeof(int);

	memcpy((char *)tableData+counter, tables, sizeOfTablesChar);
	counter += sizeOfTablesChar;

	//TODO
	rc = insertTuple("Tables", tableData, rid, counter);
	free(tableData);
	void *colData = malloc(100);
	memcpy((char *)colData+counter1, &tid2, sizeof(int));
	counter1 += sizeof(int);

	int sizeOfColumnsChar = 7;
	char columns[] = "Columns";
	//since this is varchar, have to add length
	memcpy((char *)colData+counter1, &sizeOfColumnsChar, sizeof(int));
	counter1 += sizeof(int);

	//add the table name
	memcpy((char *)colData+counter1, columns, sizeOfColumnsChar);
	counter1 += sizeOfColumnsChar;

	//add the length of file name
	memcpy((char *)colData+counter1, &sizeOfColumnsChar, sizeof(int));
		counter1 += sizeof(int);

	//add the file name
	memcpy((char *)colData+counter1, columns, sizeOfColumnsChar);
	counter1 += sizeOfColumnsChar;

	//insert this tuple to Tables table.
	rc = insertTuple("Tables", colData, rid, counter1);
	free(colData);

	vector<Attribute> tableAttrs;
	Attribute attrTable;
	attrTable.name = "TableId";
	attrTable.type = TypeInt;
	attrTable.length = (AttrLength)4;
	tableAttrs.push_back(attrTable);
	attrTable.name = "TableName";
	attrTable.type = TypeVarChar;
	attrTable.length = (AttrLength)20;
	tableAttrs.push_back(attrTable);
	attrTable.name = "FileName";
	attrTable.type = TypeVarChar;
	attrTable.length = (AttrLength)20;
	tableAttrs.push_back(attrTable);

	vector<Attribute> colAttrs;
	Attribute attrCol;
	attrCol.name = "TableId";
	attrTable.type = TypeInt;
	attrTable.length = (AttrLength)4;
	colAttrs.push_back(attrCol);
	attrCol.name = "ColumnName";
	attrTable.type = TypeVarChar;
	attrTable.length = (AttrLength)20;
	colAttrs.push_back(attrCol);
	attrCol.name = "ColumnType";
	attrTable.type = TypeVarChar;
	attrTable.length = (AttrLength)4;
	colAttrs.push_back(attrCol);
	attrCol.name = "ColumnLength";
	attrTable.type = TypeInt;
	attrTable.length = (AttrLength)4;
	colAttrs.push_back(attrCol);

	void *data = malloc(100);
	for (int i = 0; i < (int)tableAttrs.size(); i++) {
		int offset = 0;
		//add tid
		memcpy((char *)data+offset,&tid1,sizeof(int));
		offset += sizeof(int);

		int len = tableAttrs.at(i).name.length();
		memcpy((char *)data+offset,&len,sizeof(int));
		offset += sizeof(int);

		memcpy((char *)data+offset, tableAttrs.at(i).name.c_str(), len);
		offset += len;

		memcpy((char *)data+offset, &(tableAttrs.at(i).type), sizeof(int));
		offset += sizeof(int);

		memcpy((char *)data+offset, &(tableAttrs.at(i).length), sizeof(int));
		offset += sizeof(int);
		//add to the columns table
		rc = insertTuple("Columns", data, rid, offset);
	}
	free(data);

	void *data1 = malloc(100);
	for (int i = 0; i < (int)colAttrs.size(); i++) {
		int offset = 0;
		//add tid
		memcpy((char *)data1+offset,&tid2,sizeof(int));
		offset += sizeof(int);

		int len = colAttrs.at(i).name.length();
		memcpy((char *)data1+offset,&len,sizeof(int));
		offset += sizeof(int);

		memcpy((char *)data1+offset, colAttrs.at(i).name.c_str(), len);
		offset += len;

		memcpy((char *)data1+offset, &(colAttrs.at(i).type), sizeof(int));
		offset += sizeof(int);

		memcpy((char *)data1+offset, &(colAttrs.at(i).length), sizeof(int));
		offset += sizeof(int);
		//add to the columns table
		rc = insertTuple("Columns", data1, rid, offset);
	}

	free(data1);
}

RC RM::createTable(const string tableName, const vector<Attribute> &attrs) {
	int rc = 0;
	int counter = 0;

	rc = pf->CreateFile(tableName.c_str());
	if (rc == -1) return rc;

	RID rid;

	void *data = malloc(100);
	tableCount += 1;
	memcpy((char *)data+counter, &tableCount, sizeof(int));
	counter += sizeof(int);
	int tableLen = tableName.length();

	memcpy((char *)data+counter, &tableLen, sizeof(int));
	counter += sizeof(int);

	memcpy((char *)data+counter, tableName.c_str(), tableName.length());
	counter += tableName.length();

	memcpy((char *)data+counter, &tableLen, sizeof(int));
	counter += sizeof(int);

	memcpy((char *)data+counter, tableName.c_str(), tableName.length());
	counter += tableName.length();

	rc = insertTuple("Tables", data, rid, counter);
	free(data);

	void* data1 = malloc(100);
	for (int i = 0; i < (int)attrs.size(); i++) {
		counter = 0;
		memcpy((char *)data1+counter, &tableCount, sizeof(int));
		counter += sizeof(int);
		int len = (attrs.at(i).name).length();

		memcpy((char *)data1+counter, &len, sizeof(int));
		counter += sizeof(int);

		memcpy((char *)data1+counter, attrs.at(i).name.c_str(), attrs.at(i).name.length());
		counter += attrs.at(i).name.length();

		memcpy((char *)data1+counter, &(attrs.at(i).type), sizeof(int));
		counter += sizeof(int);

		memcpy((char *)data1+counter, &(attrs.at(i).length), sizeof(int));
		counter += sizeof(int);

		rc = insertTuple("Columns", data1, rid, counter);
	}
	free(data1);
	return rc;

}


RC RM::readAttribute(const string tableName, const RID &rid, const string attrName, void *data) {
	int rc = pf->CreateFile(tableName.c_str());
	if (rc != -1) {
		return -1;
	}
	vector<Attribute> tableAttrs;
	rc = getAttributes(tableName, tableAttrs);
	PF_FileHandle fileHandle;

	rc = pf->OpenFile(tableName.c_str(), fileHandle);
	if (rc == -1) {
		return rc;
	}
	int counter  = 0;
	void *tmpData = malloc(100);
	rc = readTuple(tableName, rid, tmpData);
	int len = 0, flag=0;
	for (int i = 0; i < (int)tableAttrs.size() && flag == 0; i++) {
		Attribute curr = tableAttrs.at(i);
		if (curr.type == TypeVarChar) {
			memcpy(&len, (char *)tmpData+counter, sizeof(int));
			counter += sizeof(int);
			memcpy((char *)data, (char *)tmpData+counter, len);
			counter += len;
		} else {
			len = sizeof(int);
			memcpy((char *)data, (char *)tmpData+counter, len);
			counter += len;
		}
		if (curr.name == attrName) {
			flag = 1;
		}
	}
	free(tmpData);
	return rc;
}

RC RM::getAttributes(const string tableName, vector<Attribute> &attrs)
{
		PF_FileHandle fileHandle;
		int r = pf->OpenFile(tableName.c_str(),fileHandle);
		if(r == -1)
		{
			pf->CloseFile(fileHandle);
			return -1;
		}
		pf->CloseFile(fileHandle);
		attrs.clear();

		int rc,tid1,tid2;
		tid1 = getTableId(tableName);

		int i,nop;
		int col_name_length,col_type,col_length;
		char *col_name = (char*)malloc(100);
		rc = pf->OpenFile("Columns",fileHandle);
		void *data = malloc(PF_PAGE_SIZE);
		int offset,j;
		Attribute attr;

		nop = fileHandle.GetNumberOfPages();
		for(i=0;i<nop;i++) {
			rc = fileHandle.ReadPage(i,data);
			vector<Slot> slots;
			getSlotsForPage((const void*)data, slots);
			for(j=1;j< (int)slots.size();j++) {
				offset = slots[j].recOffset;
				memcpy(&tid2,(char*)data+offset,sizeof(int));
				offset+=sizeof(int);
				if(tid2==tid1) {
					memcpy(&col_name_length,(char*)data+offset,sizeof(int));
					offset+=sizeof(int);
					memcpy(col_name,(char*)data+offset,col_name_length);
					col_name[col_name_length]='\0';
					attr.name = col_name;
					offset+=col_name_length;
					memcpy(&col_type,(char*)data+offset,sizeof(int));
					offset+=sizeof(int);
					attr.type = (AttrType)col_type;
					memcpy(&col_length,(char*)data+offset,sizeof(int));
					offset+=sizeof(int);
					attr.length = col_length;
					attrs.push_back(attr);
				}
			}
		}

		rc = pf->CloseFile(fileHandle);
		free(data);
		free(col_name);
		return rc;
}


RC RM::insertTuple(const string tableName, const void *data, RID &rid, int tupleSize)
{
	int rc;
	PF_FileHandle fHandle, bitFileHandle;
	vector<Attribute> cAttribute;
	unsigned int attrIndx, fNumPages, freePageNum, bitPageNum;
	int bitPageOffset = 0, totalSlots, minTupleSize = 0, defaultTupleSize = 0, minStringLen;
	void *bitData, *freePage;
	Slot freeSpace, newSlot;
	bool isFreePage;
	char bitArray = 0, bitIndx;

	//getAttributes returns -1 on failure
	if (tupleSize == 0) {
		rc = getAttributes(tableName, cAttribute);
		if(rc == -1)
			return rc;
	}

	//open table file
	rc = pf->OpenFile(tableName.c_str(), fHandle);
	if(rc == -1){
		//create file if not present
		rc = pf->CreateFile(tableName.c_str());
		if (rc == -1) {
			return rc;
		} else {
			rc = pf->OpenFile(tableName.c_str(), fHandle);
			if (rc == -1) return rc;
		}
	}
	freePage = malloc(PF_PAGE_SIZE);


	//calculate tuple size if not passed.
	minStringLen = 4;
	if(tupleSize == 0) {
		for(attrIndx = 0; attrIndx < cAttribute.size(); attrIndx++) {
			if(cAttribute[attrIndx].type == TypeVarChar) {
				tupleSize += *((int *) ((char *) data+tupleSize));
				defaultTupleSize += minStringLen;
			}
			tupleSize += 4;
			defaultTupleSize += 4;
		}
	}
	else {
			defaultTupleSize = 20;
	}
	minTupleSize = defaultTupleSize + slotSize;

	// open bitmap file
	rc = pf->OpenFile((tableName+"BitMap").c_str(), bitFileHandle);
	if (rc == -1) {
		//create file if not present
		rc = pf->CreateFile((tableName+"BitMap").c_str());
		if (rc == -1) {
			return rc;
		} else {
			rc = pf->OpenFile((tableName+"BitMap").c_str(), bitFileHandle);
			if (rc == -1) return rc;
		}
	}

	bitData = calloc(PF_PAGE_SIZE, 1);
	fNumPages = fHandle.GetNumberOfPages();
	isFreePage = false;
	bitPageNum = 0;
	freePageNum = 0;
	bitIndx = 128;
	if(fNumPages != 0) {
		while(!isFreePage && (freePageNum < fNumPages)){
			bitFileHandle.ReadPage(bitPageNum++, bitData);
			bitPageOffset = 0;
			while(!isFreePage && bitPageOffset < PF_PAGE_SIZE && freePageNum < fNumPages){
				memcpy(&bitArray, (const char *)((char *)bitData+bitPageOffset++), 1);
					bitIndx = 128;
					while((bitArray & bitIndx) && freePageNum < fNumPages) {
						bitIndx >>= 1;
						freePageNum++;
					}
					if(bitIndx && freePageNum < fNumPages){
						fHandle.ReadPage(freePageNum, freePage);
						freeSpace = *((Slot *) ((char *)freePage+PF_PAGE_SIZE-slotSize));
						if(freeSpace.recLength >= (tupleSize+slotSize)){
							isFreePage = true;
						}
					}
			}
		}
	}
	if(!isFreePage){
		fNumPages++;
		freeSpace.recLength = PF_PAGE_SIZE-slotSize;
		freeSpace.recOffset = 0;
		totalSlots = 1;
	}
	else {
		bitPageNum--;
		bitPageOffset--;
		rc = getNumOfSlotsForPage(freePage, totalSlots);
	}

	//add tuple and update corresponding slot
	memcpy(((char *)freePage+freeSpace.recOffset), (char *) data, tupleSize);
	newSlot.recOffset = freeSpace.recOffset;
	//tupleSlot.recOffset = freeSpace.recOffset;
	newSlot.recLength = tupleSize;
	//tupleSlot.recLength = tupleSize;
	freeSpace.recOffset += tupleSize;
	freeSpace.recLength -= (tupleSize+slotSize);
	memcpy((Slot *)((char *)freePage+PF_PAGE_SIZE-slotSize), (Slot *) &freeSpace, slotSize);
	memcpy((Slot *)((char *)freePage+PF_PAGE_SIZE-(totalSlots+1)*slotSize), (Slot *) &newSlot, slotSize);
	rid.pageNum = freePageNum;
	rid.slotNum = totalSlots+1;

	if(!isFreePage){
		fHandle.AppendPage(freePage);
	}
	else {
		fHandle.WritePage(freePageNum, freePage);
	}

	//update bitMap file that page is full
	if(freeSpace.recLength <= minTupleSize || !isFreePage){
		bitArray |= bitIndx;
		if(!isFreePage)
			bitArray = 0;
		memcpy(((char *)bitData+bitPageOffset), &bitArray, 1);
		if(!isFreePage){
			bitFileHandle.AppendPage(bitData);
		}
		else {
			bitFileHandle.WritePage(bitPageNum, bitData);
		}
	}

	free(freePage);
	free(bitData);
	pf->CloseFile(fHandle);
	pf->CloseFile(bitFileHandle);
	return 0;
}

RC RM::deleteTuples(const string tableName){
	Slot freeSpace;
	PF_FileHandle fHandle, bitFileHandle;
	char singlePage = 128;
	void *fData, *bitFileData;

	pf->DestroyFile(tableName.c_str());
	pf->DestroyFile((tableName+"BitMap").c_str());
	freeSpace.recLength = PF_PAGE_SIZE - slotSize;
	freeSpace.recOffset = 0;
	pf->CreateFile(tableName.c_str());
	pf->CreateFile((tableName+"BitMap").c_str());
	pf->OpenFile(tableName.c_str(), fHandle);
	pf->OpenFile((tableName+"BitMap").c_str(), bitFileHandle);
	fData = malloc(PF_PAGE_SIZE);
	bitFileData = malloc(PF_PAGE_SIZE);
	*((Slot *)((char *)fData+PF_PAGE_SIZE-slotSize)) = freeSpace;
	*((char *) bitFileData) = singlePage;
	fHandle.AppendPage(fData);
	bitFileHandle.AppendPage(bitFileData);
	free(fData);
	free(bitFileData);
	pf->CloseFile(fHandle);
	pf->CloseFile(bitFileHandle);
	return 0;
}

RC RM::deleteTuple(const string tableName, const RID &rid){
	PF_FileHandle fHandle;
	int pageOffset = 0;
	void *fData;

	fData = malloc(PF_PAGE_SIZE);
	pf->OpenFile(tableName.c_str(), fHandle);
	fHandle.ReadPage(rid.pageNum, fData);
	pageOffset = PF_PAGE_SIZE - (rid.slotNum)*slotSize;
	((Slot *) ((char *)fData+pageOffset))->recLength = -1;
	//((Slot *) ((char *)fData+pageOffset))->recOffset = 0;
	fHandle.WritePage(rid.pageNum, fData);
	free(fData);
	pf->CloseFile(fHandle);
	return 0;
}

RC RM::updateTuple(const string tableName, const void *data, const RID &rid){
	PF_FileHandle fHandle;
	int pageOffset = 0, pageOffset1 = 0;
	unsigned int attrIndx;
	int curTupleSize, upTupleSize;
	void *fData, *fData1;
	vector<Attribute> cAttribute;
	RID ridAux;

	//calculate tuple size of current tuple in file
	fData = malloc(PF_PAGE_SIZE);
	pf->OpenFile(tableName.c_str(), fHandle);
	fHandle.ReadPage(rid.pageNum, fData);
	pageOffset = PF_PAGE_SIZE - (rid.slotNum+1)*slotSize;
	curTupleSize = ((Slot *) ((char *)fData+pageOffset))->recLength;

	//calculate tuple size of new tuple
	if(getAttributes(tableName, cAttribute) == -1)
			return -1;

	for(attrIndx = 0; attrIndx < cAttribute.size(); attrIndx++){
		if(cAttribute[attrIndx].name == "varchar"){
			upTupleSize += *((int *) ((char *)data+upTupleSize));
		}
		upTupleSize += 4;
	}

	if(curTupleSize >= upTupleSize){
		memcpy((char *)fData+pageOffset, data, upTupleSize);
		((Slot *) ((char *)fData+pageOffset))->recLength = upTupleSize;
	}
	else {
		int pageDiff = 0;
		fData1 = malloc(PF_PAGE_SIZE);
		insertTuple(tableName, data, ridAux, 0);

		fHandle.ReadPage(ridAux.pageNum, fData1);
		pageOffset1 = PF_PAGE_SIZE - (ridAux.slotNum)*slotSize;
		fHandle.ReadPage(rid.pageNum, fData);
		pageOffset = PF_PAGE_SIZE - (rid.slotNum)*slotSize;
		pageDiff = (ridAux.pageNum - rid.pageNum)*PF_PAGE_SIZE;
		((Slot *) ((char *) fData+pageOffset))->recLength = ((Slot *) ((char *) fData1+pageOffset1))->recLength;
		((Slot *) ((char *) fData+pageOffset))->recOffset = pageDiff + ((Slot *) ((char *) fData1+pageOffset1))->recOffset;
		fHandle.WritePage(rid.pageNum, fData);
	}

	free(fData1);
	free(fData);
	pf->CloseFile(fHandle);

	return 0;
}

RC RM::readTuple(const string tableName, const RID &rid, void *data){
	PF_FileHandle fHandle;
	void *fData;
	int rc = 0, pageOffset;
	fData = malloc(PF_PAGE_SIZE);
	pf->OpenFile(tableName.c_str(), fHandle);
	fHandle.ReadPage(rid.pageNum, fData);
	vector<Slot> slots;
	unsigned int numSlots = getSlotsForPage(fData, slots);
	if (numSlots >= rid.slotNum) {
		Slot s = slots.at(rid.slotNum-1);
		if (s.recLength == -1) {
			free(fData);
			pf->CloseFile(fHandle);
			return -1;
		}
		pageOffset = s.recOffset;
		if(pageOffset < 0 || pageOffset >= PF_PAGE_SIZE){
			int pOffset = pageOffset, pageDiff = 0, pageNum;
			short sign = 1;
			sign = (pOffset < 0 ? -1 : 1);
			pOffset *= sign;
			pageOffset = pOffset % PF_PAGE_SIZE;
			pageDiff = pOffset/PF_PAGE_SIZE;
			if(sign == -1){
				pageDiff++;
				pageOffset = PF_PAGE_SIZE - pageOffset;
			}
			pageNum = rid.pageNum+sign*pageDiff;
			fHandle.ReadPage(pageNum, fData);
		}
		memcpy((char *)data, (char *)fData+pageOffset, s.recLength);
	} else {
		rc = -1;
	}
	free(fData);
	pf->CloseFile(fHandle);
	return rc;
}


//add this function to RM class
RC RM::getTableId(const string tableName){
	void *tData;
	PF_FileHandle fHandle;
	string tFile = "Tables", cFile = "Columns";
	char *tName;
	vector<Slot> slots;
	int totalSlots, vectorIndx;
	int tNameLen;

	tData = malloc(PF_PAGE_SIZE);
	pf->OpenFile(tFile.c_str(), fHandle);
	fHandle.ReadPage(0, tData);
	totalSlots = getSlotsForPage((const void *) tData, slots);
	if(!totalSlots){
		return -1;
	}
	for(vectorIndx=1; vectorIndx<totalSlots; vectorIndx++){
		void *recData = malloc(slots[vectorIndx].recLength);
		memcpy((char *) recData, (char *) tData+slots[vectorIndx].recOffset, slots[vectorIndx].recLength);
		tNameLen = *((int *) ((char *)recData+sizeof(int)));
		tName = (char *) malloc(tNameLen);
		memcpy(tName, (char *)recData+2*sizeof(int), tNameLen);
		tName[tNameLen] = '\0';
		if(((string) tName) == tableName){
			free(tData);
			pf->CloseFile(fHandle);
			return *((int *) recData);
		}
	}

	free(tData);
	pf->CloseFile(fHandle);
	return -1;
}

RC getSlotsForPage(const void *data, vector<Slot> &slots){
	if(data == NULL)
			return -1;

	int slotNum, totalSlots;
	Slot slotAux;

	getNumOfSlotsForPage(data, totalSlots);
	for(slotNum = 0; slotNum < totalSlots; slotNum++) {
		slotAux = *((Slot *) ((char *)data+PF_PAGE_SIZE-(slotNum+1)*slotSize));
		slots.push_back(slotAux);
	}

	return totalSlots;
}

RC getNumOfSlotsForPage(const void *data, int &ret) {
	if(data == NULL)
			return -1;

	Slot slotAux;

	slotAux = *((const Slot *) ((char *)data+PF_PAGE_SIZE-slotSize));
	ret =  ((PF_PAGE_SIZE-slotAux.recOffset-slotAux.recLength)/slotSize);
	return 0;
}

RC RM::deleteTable(const string tableName) {
	int rc;
	rc = pf->DestroyFile(tableName.c_str());
	if (rc == -1) return rc;
	rc = pf->DestroyFile((tableName+"BitMap").c_str());
	if (rc == -1) return rc;
	RM_ScanIterator rmScanIter;
	vector<string> attrNames;
	string tid = "TableId";
	int tableId = getTableId(tableName);
	attrNames.push_back(tid);
	int nameLen = (int)tableName.length();
	void *data = malloc(nameLen);
	memcpy((char *)data, tableName.c_str(), nameLen);
	scan("Tables", "TableName", EQ_OP, data,attrNames, rmScanIter);
	int numRids = (int)rmScanIter.rids.size();
	cout << numRids << endl;
	for (int i = 0; i < numRids; i++) {
		deleteTuple("Tables", rmScanIter.rids.at(i));
	}
	free(data);
	//end of removing table record from Tables.
	void *data1 = malloc(4);
	memcpy((char *)data1, &tableId, sizeof(int));
	scan("Columns", "TableId", EQ_OP, data1, attrNames, rmScanIter);
	int numColRids = (int)rmScanIter.rids.size();
	cout << numColRids << endl;
	for (int i = 0; i < numColRids; i++) {
		deleteTuple("Columns", rmScanIter.rids.at(i));
	}
	free(data1);
	return 0;
}

RC RM::scan(const string tableName,const string conditionAttribute,const CompOp compOp,const void *value,const vector<string> &attributeNames,RM_ScanIterator &rm_ScanIterator) {
	rm_ScanIterator.resetState();
	PF_FileHandle fileHandle;
	int rc;
	rc = pf->OpenFile(tableName.c_str(), fileHandle);
	if (rc == -1) return rc;
	int nop;
	RID rid;
	nop = fileHandle.GetNumberOfPages();
	void *data = malloc(PF_PAGE_SIZE);
	void *record = malloc(1000);
	void *projection = malloc(PF_PAGE_SIZE);
	char *projection_cstring = (char*)malloc(1000);
	vector<Attribute> attrs;
	rc = getAttributes(tableName.c_str(), attrs);
	if (rc == -1) return rc;
	int k;
	for (k = 0; k < (int)attrs.size(); k++) {
		if (isAttributeValid(attrs.at(k).name, attributeNames) == 0) {
			rm_ScanIterator.addAttribute(attrs.at(k));
		}
	}
	int i,j, flag, offset,x,y,pro_offset;
	vector<Slot>slots;
	for (i =0; i < nop; i++) {
		rc =fileHandle.ReadPage(i, data);
		int numSlots = getSlotsForPage(data, slots);
		for (j = 1; j <numSlots ; j++) {
			if (slots.at(j).recLength == -1) continue;
			offset = 0;
			memcpy((char*)record,(char*)data+slots.at(j).recOffset,slots.at(j).recLength);
			flag = 0;
			for (k = 0; k < (int)attrs.size(); k++) {
				if (conditionAttribute.length() == 0) {
					flag =1;
				} else if (attrs.at(k).name.compare(conditionAttribute) == 0) {
					if (attrs.at(k).type != TypeVarChar) {
						memcpy(&x, (char *)record+offset, sizeof(int));
						offset += sizeof(int);
						memcpy(&y, (char *)value, sizeof(int));
						switch(compOp)
						{
							case EQ_OP:
								if(x == y) flag=1;break;
							case LT_OP:
								if(x<y) flag = 1;;break;
							case GT_OP:
								if(x>y) flag = 1;break;
							case LE_OP:
								if(x<=y) flag = 1;break;
							case GE_OP:
								if(x>=y) flag = 1;break;
							case NE_OP:
								if(x!=y) flag = 1;break;
							case NO_OP:
								flag = 1; break;
						}

					} else {
						char *s1 =(char*) malloc(1000);
						memcpy(&x,(char*)record+offset,sizeof(int));
						offset+=sizeof(int);
						memcpy((char*)s1,(char*)record+offset,x);
						offset+=x;

						s1[x] = '\0';

						y = strcmp(s1,(char*)value);
						switch(compOp)
						{
							case EQ_OP:
								if(y==0) flag=1;break;
							case LT_OP:
								if(y<0) flag = 1;;break;
							case GT_OP:
								if(y>0) flag = 1;break;
							case LE_OP:
								if(y<=0) flag = 1;break;
							case GE_OP:
								if(y>=0) flag = 1;break;
							case NE_OP:
								if(y!=0) flag = 1;break;
							case NO_OP:
								flag = 1; break;
						}


						free(s1);
					}
				} else {
					if(attrs.at(k).type == TypeVarChar) {
						memcpy(&x,(char*)record+offset,sizeof(int));
						offset+=sizeof(int);
						offset+=x;
					} else {
						offset+=sizeof(int);
					}
				}
			}

			if (flag == 0) continue;
			offset = 0;
			pro_offset = 0;
			rid.pageNum = i;
			rid.slotNum = j;
			rm_ScanIterator.addRid(rid);
			for(k=0;k<(int)attrs.size();k++) {
				if(isAttributeValid(attrs.at(k).name,attributeNames) == 0) {
					if(attrs.at(k).type == TypeVarChar) {
						memcpy(&x,(char*)record+offset,sizeof(int));
						offset+=sizeof(int);

						memcpy((char*)projection+pro_offset,&x,sizeof(int));
						pro_offset+=sizeof(int);

						memcpy((char*)projection+pro_offset,(char*)record+offset,x);
						offset+=x;
						pro_offset+=x;
					} else {
						memcpy((char*)projection+pro_offset,(char*)record+offset,sizeof(int));
						offset+=sizeof(int);
						pro_offset+=sizeof(int);
					}
				} else {
					if(attrs.at(k).type == TypeVarChar) {
						memcpy(&x,(char*)record+offset,sizeof(int));
						offset+=sizeof(int);
						offset+=x;
					} else {
						offset+=sizeof(int);
					}
				}
			}
			rm_ScanIterator.addData(projection,pro_offset);
		}
	}
	free(projection);
	free(record);
	free(data);
	free(projection_cstring);
	rc = pf->CloseFile(fileHandle);
	return rc;
}

int isAttributeValid(const string attribute, const vector<string> attrNames) {
	for (int i = 0; i < (int)attrNames.size(); i++) {
		if (attrNames.at(i) == attribute) return 0;
	}
	return -1;
}

RM_ScanIterator::RM_ScanIterator() {
	pf = PF_Manager::Instance();
	iterator=0;
}

RC RM_ScanIterator::getNextTuple(RID &rid,void *data) {
	if(iterator == (int)rids.size())
		return RM_EOF;
	rid = rids.at(iterator);
	memcpy((char*)data,(char*)data_projection.at(iterator),lengths.at(iterator));
	iterator++;
	return 1;
}

void RM_ScanIterator::addAttribute(Attribute attr) {
	attrs.push_back(attr);
}

void RM_ScanIterator::addRid(RID rid) {
	rids.push_back(rid);
}

RC RM::reorganizePage(const string tableName, unsigned int pageNumber) {
	int rc;
	PF_FileHandle handler;
	rc = pf->OpenFile(tableName.c_str(), handler);
	if (rc == -1) return rc;
	void *data = malloc(PF_PAGE_SIZE);
	handler.ReadPage(pageNumber, data);
	vector<Slot> slots;
	getSlotsForPage(data, slots);
	int totalFreeSpace = 0;
	for (int i=1; i < (int)slots.size()-1; i++) {
		if (slots.at(i).recLength == -1) {
			int nextOffset = slots.at(i+1).recOffset;
			totalFreeSpace += (nextOffset - slots.at(i).recOffset);
			cout << slots.at(i).recOffset << endl;
			cout << nextOffset << endl;
			for (int j = i+1; j < (int)slots.size(); j++) {
				if (slots.at(j).recLength == -1) break;
				void *tempData = malloc(slots.at(j).recLength);
				memcpy((char *)tempData, (char *)data+slots.at(j).recOffset, slots.at(j).recLength);
				int freeSpaceOffset = slots.at(j).recOffset - totalFreeSpace;
				memcpy((char *)data+freeSpaceOffset, (char *)tempData, slots.at(j).recLength);
				int slotOffset = PF_PAGE_SIZE - slotSize*(j+1) ;
				memcpy((char *)data+slotOffset, &freeSpaceOffset, sizeof(int));
				free(tempData);
			}
		}
	}
	handler.WritePage(pageNumber, data);
	free(data);
	return 0;
}

void RM_ScanIterator::addData(void *projection,int length)
{
	void * tempCopy = malloc( length );
	memcpy( ( char * )tempCopy, ( const char * )projection, length );
	data_projection.push_back(tempCopy);
	lengths.push_back(length);
}

void RM_ScanIterator::resetState() {
	iterator=0;
	for( std::vector< void * > ::iterator iter = data_projection.begin(); iter != data_projection.end(); ++iter )
	{
		free( ( void * )( *iter ) );
	}
	attrs.clear();
	rids.clear();
	data_projection.clear();
}
