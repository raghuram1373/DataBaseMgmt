#include "pf.h"
#include <iostream>
#include <fstream>
#include <math.h>

PF_Manager* PF_Manager::_pf_manager = 0;


PF_Manager* PF_Manager::Instance()
{
    if(!_pf_manager)
        _pf_manager = new PF_Manager();
    
    return _pf_manager;    
}


PF_Manager::PF_Manager()
{
}


PF_Manager::~PF_Manager()
{
}

    
RC PF_Manager::CreateFile(const char *fileName)
{
	ifstream ifile(fileName);
	if (!ifile) {
		ifile.close();
		FILE * pFile;
		pFile = fopen (fileName,"w");
		fclose(pFile);
		return 0;
	} else {
		ifile.close();
		return -1;
	}

}


RC PF_Manager::DestroyFile(const char *fileName)
{
	ifstream ifile(fileName);
	if (ifile) {
		ifile.close();
		if(remove(fileName)) {
			cout << remove(fileName) << endl;
			cout << "cannot delete" << endl;
			return -1;
		} else {
			cout << "deleted successfully" << endl;
			return 0;
		}
	} else {
		return -1;
	}
}


RC PF_Manager::OpenFile(const char *fileName, PF_FileHandle &fileHandle)
{
	ifstream ifile(fileName);
	if (ifile) {
		ifile.close();
		fileHandle.stream.open(fileName, ios::out | ios::in | ios::binary);
		return 0;
	} else {
		ifile.close();
		return -1;
	}
}


RC PF_Manager::CloseFile(PF_FileHandle &fileHandle)
{
	if (fileHandle.stream.is_open()) {
		fileHandle.stream.close();
		return 0;
	} else {
		return -1;
	}
}


PF_FileHandle::PF_FileHandle()
{
}
 

PF_FileHandle::~PF_FileHandle()
{
	if (stream.is_open()) {
		stream.close();
	}
}


RC PF_FileHandle::ReadPage(PageNum pageNum, void *data)
{
	if (stream.is_open()) {
		if (pageNum < 0 || pageNum >= GetNumberOfPages()) {
			return -1;
		} else {
			stream.seekg(pageNum*PF_PAGE_SIZE, ios::beg);
			stream.read((char*)data, PF_PAGE_SIZE);
			return 0;
		}
	} else {
		return -1;
	}
}


RC PF_FileHandle::WritePage(PageNum pageNum, const void *data)
{
	if (stream.is_open()) {
		if (pageNum < 0 || pageNum >= GetNumberOfPages()) {
			return -1;
		} else {
			stream.seekg(pageNum*PF_PAGE_SIZE, ios::beg);
			stream.write((char*)data, PF_PAGE_SIZE);
			return 0;
		}
	} else {
		return -1;
	}
}


RC PF_FileHandle::AppendPage(const void *data)
{
	if (stream.is_open()){
		stream.seekg(0, ios::end);
		stream.write((char*)data, PF_PAGE_SIZE);
		return 0;
	} else {
		return -1;
	}
}


unsigned PF_FileHandle::GetNumberOfPages()
{
	stream.seekg(0, fstream::end);
	int size = stream.tellg();
	int numberPages = size/PF_PAGE_SIZE;
	return numberPages;
}
