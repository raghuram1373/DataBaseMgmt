# include "qe.h"
#include <sstream>
#include<cmath>
using namespace std;

union attrVals {
	int i;
	float f;
	char *s;
};

RC GetAttrVal(const void *data1, vector<Attribute> attr, union attrVals &attrVal, int &offset1, int &atrType, Condition condition, bool isLeftArg);

Filter::Filter(Iterator *input, const Condition &condition) {
	void *data = malloc(1000);
	input->getAttributes(attrs);
	iterCount = 0;
	count = 0;
	while (input->getNextTuple(data) != QE_EOF) {
		int offset = 0;
		int flag = 0;
		for (int i = 0 ; i < (int)attrs.size(); i++) {
			if (condition.lhsAttr == attrs[i].name) {
				if (attrs[i].type == TypeVarChar) {
					string x;
					int len=0;
					string y;
					int len1 = 0;
					memcpy(&len, (char *)data+offset, sizeof(int));
					offset += sizeof(int);
					memcpy(&x, (char *)data+offset, len);
					if (condition.bRhsIsAttr == false) {
						//check if condition is satisfied
						memcpy(&len1, (char *)(condition.rhsValue.data), sizeof(int));
						memcpy(&y, (char *)condition.rhsValue.data+sizeof(int), len1);

					} else {
						int offset1 = 0;
						for (int j = 0; j < (int)attrs.size(); j++) {
							if (attrs[j].name == condition.rhsAttr) {
								if (attrs[j].type != attrs[i].type) {
									memcpy(&len1, (char *)data+offset1, sizeof(int));
									offset1 += sizeof(int);
									memcpy(&y, (char *)data+offset1, len1);
								}
							} else {
								if (attrs[j].type == TypeVarChar) {
									int l;
									memcpy(&l, (char *)data+offset1, sizeof(int));
									offset1 += (l+4);
								} else {
									offset1 += 4;
								}
							}
						}
					}
					switch(condition.op) {
						case EQ_OP:
							if(x == y) flag=1;break;
						case LT_OP:
							if(x<y) flag = 1;break;
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
					offset += sizeof(int)+len;
				} else if (attrs[i].type == TypeReal) {
					float x,y;
					memcpy(&x, (char *)data+offset, sizeof(float));
					if (condition.bRhsIsAttr == false) {
						memcpy(&y, (char *)(condition.rhsValue.data), sizeof(float));
					} else {
						int offset1 = 0;
						for (int j = 0; j < (int)attrs.size(); j++) {
							if (attrs[j].name == condition.rhsAttr) {
								if (attrs[j].type != attrs[i].type) {
									memcpy(&y, (char *)data+offset1, sizeof(float));
								}
							} else {
								if (attrs[j].type == TypeVarChar) {
									int l;
									memcpy(&l, (char *)data+offset1, sizeof(int));
									offset1 += (l+4);
								} else {
									offset1 += 4;
								}
							}
						}
					}
					switch(condition.op) {
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
					offset += sizeof(float);
				} else {
					int x,y;
					memcpy(&x, (char *)data+offset, sizeof(int));
					if (condition.bRhsIsAttr == false) {
						memcpy(&y, (char *)(condition.rhsValue.data), sizeof(int));
					} else {
						int offset1 = 0;
						for (int j = 0; j < (int)attrs.size(); j++) {
							if (attrs[j].name == condition.rhsAttr) {
								if (attrs[j].type != attrs[i].type) {
									memcpy(&y, (char *)data+offset1, sizeof(int));
								}
							} else {
								if (attrs[j].type == TypeVarChar) {
									int l;
									memcpy(&l, (char *)data+offset1, sizeof(int));
									offset1 += (l+4);
								} else {
									offset1 += 4;
								}
							}
						}
					}
					switch(condition.op) {
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
					offset += sizeof(int);
				}
			} else {
				if (attrs[i].type == TypeVarChar) {
					int l;
					memcpy(&l, (char *)data+offset, sizeof(int));
					offset += (l+4);
				} else {
					offset += 4;
				}
			}
		}
		if (flag == 1) {
			//push tuple to filterdata;
			void *temp = malloc(offset);
			memcpy((char *) temp, (char *)data, offset);
			filterData.push_back(temp);
			len.push_back(offset);
			count++;
		}
	}
	free(data);
}

RC Filter::getNextTuple(void *data) {
	if (iterCount < count) {
		memcpy((char *)data, (char *)(filterData.at(iterCount)), len.at(iterCount));
		iterCount++;
		return 0;
	} else {
		return QE_EOF;
	}
}

Filter::~Filter() {
	for (int i = 0 ; i < (int)filterData.size(); i++) {
		free(filterData.at(i));
	}
	filterData.clear();
}

void Filter::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->attrs;
}

Project::Project(Iterator *input, const vector<string> &attrNames) {
	vector<Attribute> attrs;
	input->getAttributes(attrs);
	void *data = malloc(1000);
	count = 0;
	iterCount = 0;
	for (int i = 0; i < (int)attrNames.size(); i++) {
		for (int j = 0; j < (int)attrs.size(); j++) {
			if (attrs[j].name == attrNames[i]) {
				Attribute a;
				a.length = attrs[j].length;
				a.name = attrs[j].name;
				this->attrs.push_back(a);
			}
		}
	}
	while (input->getNextTuple(data) != QE_EOF) {
		int offset1 = 0;
		void *tempProject = malloc(1000);
		for (int i = 0; i < (int)attrNames.size(); i++) {
			int offset = 0;
			for (int j = 0; j < (int)attrs.size(); j++) {
				if (attrs[j].name == attrNames[i]) {
					if (attrs[j].type == TypeVarChar) {
						int len;
						memcpy(&len, (char *)data+offset, sizeof(int));
						memcpy((char *)tempProject+offset1, (char *)data+offset, (sizeof(int)+len));
						offset += (len + 4);
						offset1 += (len + 4);
					} else {
						memcpy((char *)tempProject+offset1, (char *)data+offset, sizeof(int));
						offset += sizeof(int);
						offset1 += sizeof(int);
					}
				} else {
					if (attrs[j].type == TypeVarChar) {
						int l;
						memcpy(&l, (char *)data+offset, sizeof(int));
						offset += (l+4);
					} else {
						offset += 4;
					}
				}
			}
		}
		dataProject.push_back(tempProject);
		len.push_back(offset1);
		count++;
	}
	free(data);
}

Project::~Project() {
	for (unsigned int i = 0; i < dataProject.size(); i++) {
		free(dataProject.at(i));
	}
	dataProject.clear();
}

void Project::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->attrs;
}

RC Project::getNextTuple(void *data) {
	if (iterCount < count) {
		memcpy((char *)data, (char *)(dataProject.at(iterCount)), len.at(iterCount));
		iterCount++;
		return 0;
	} else {
		return QE_EOF;
	}
}

NLJoin::NLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, unsigned int numPages) {
	vector<Attribute> leftAttrs;
	leftIn->getAttributes(leftAttrs);
	leftIn->getAttributes(attrs);
	vector<Attribute> rightAttrs;
	rightIn->getAttributes(rightAttrs);
	count = 0;
	iterCount = 0;
	for (int i = 0; i < (int)rightAttrs.size(); i++) {
		attrs.push_back(rightAttrs[i]);
	}
	void *data1 = malloc(1000);
	void *temp1 = malloc(1000);

	while (leftIn->getNextTuple(data1) != QE_EOF) {
		if (condition.bRhsIsAttr == true) {
			int offset1=0;
			int len1;
			float f1;
			for (int i = 0; i < (int)leftAttrs.size(); i++) {
				if (leftAttrs[i].name == condition.lhsAttr) {
					if (leftAttrs[i].type == TypeVarChar) {
						memcpy(&len1, (char *)data1+offset1, sizeof(int));
						memcpy((char *)temp1, ((char *)data1+offset1+4), len1);
						offset1 += (len1 + 4);
					} else if (leftAttrs[i].type == TypeInt) {
						memcpy(&len1, (char *)data1+offset1, sizeof(int));
						offset1 += 4;
					} else {
						memcpy(&f1, (char *)data1+offset1, sizeof(float));
						offset1 += 4;
					}
				} else {
					if (leftAttrs[i].type == TypeVarChar) {
						int tempLen;
						memcpy(&tempLen, (char *)data1+offset1, sizeof(int));
						offset1 += (4+tempLen);

					} else {
						offset1 += 4;
					}
				}
			}
			void* data2 = malloc(1000);
			rightIn->setIterator();
			while (rightIn->getNextTuple(data2) != QE_EOF) {
				int offset2=0;
				int flag = 0;
				for (int j = 0; j < (int)rightAttrs.size(); j++) {
					if (rightAttrs[j].name == condition.rhsAttr) {
						if (rightAttrs[j].type == TypeVarChar) {
							string x;
							string y;
							int tmpLen;
							memcpy(&tmpLen, (char *)data2+offset2, sizeof(int));
							offset2 += 4;
							memcpy(&y, (char *)data2+offset2, tmpLen);
							offset2 += tmpLen;
							memcpy(&x, (char *)temp1, len1);
							switch(condition.op) {
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
						} else if (rightAttrs[j].type == TypeReal) {
							float x;
							float y;
							x = f1;
							memcpy(&y, (char *)data2+offset2, sizeof(float));
							offset2 += 4;
							switch(condition.op) {
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
							int x;
							int y;
							x = len1;
							memcpy(&y, (char *)data2+offset2, sizeof(int));
							offset2 += 4;
							switch(condition.op) {
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
						}
					} else {
						if (rightAttrs[j].type == TypeVarChar) {
							int tempLen;
							memcpy(&tempLen, (char *)data2+offset2, sizeof(int));
							offset2 += (4+tempLen);

						} else {
							offset2 += 4;
						}
					}
				} // end of for
				if (flag == 1) {
					void *temp = malloc(1500);
					memcpy((char *)temp, (char *)data1, offset1);
					memcpy((char *)temp+offset1, (char *)data2, offset2);
					nlJoin.push_back(temp);
					len.push_back(offset1+offset2);
					count++;
				}
			} // end of second iter while
			free(data2);
		}
	}
	free(data1);
}

NLJoin::~NLJoin() {
	for (int i = 0; i < (int)nlJoin.size(); i++) {
		free(nlJoin.at(i));
	}
	nlJoin.clear();
}

RC NLJoin::getNextTuple(void *data) {
	if (iterCount < count) {
		memcpy((char *)data, (char *)(nlJoin.at(iterCount)), len.at(iterCount));
		iterCount++;
		return 0;
	} else {
		return QE_EOF;
	}
}

void NLJoin::getAttributes(vector<Attribute> &attrs) const{
	attrs.clear();
	attrs = this->attrs;
}

INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition, unsigned int numPages) {
	vector<Attribute> leftAttrs;
//	int rhsValue;
	leftIn->getAttributes(leftAttrs);
	leftIn->getAttributes(attrs);
	vector<Attribute> rightAttrs;
	rightIn->getAttributes(rightAttrs);
	count = 0;
	iterCount = 0;
	for (int i = 0; i < (int)rightAttrs.size(); i++) {
		attrs.push_back(rightAttrs[i]);
	}
	void *data1 = malloc(1000);
	while (leftIn->getNextTuple(data1) != QE_EOF) {
		if (condition.bRhsIsAttr == true) {
			int offset1=0;
			int len1;
			float f1;
			void *temp1 = malloc(1000);
			int lhsAttrType =  TypeInt;
			for (int i = 0; i < (int)leftAttrs.size(); i++) {
				if (leftAttrs[i].name == condition.lhsAttr) {
					if (leftAttrs[i].type == TypeVarChar) {
						memcpy(&len1, (char *)data1+offset1, sizeof(int));
						memcpy((char *)temp1, ((char *)data1+offset1+4), len1);
						offset1 += (len1 + 4);
						lhsAttrType = TypeVarChar;
					} else if (leftAttrs[i].type == TypeInt) {
						memcpy(&len1, (char *)data1+offset1, sizeof(int));
						offset1 += 4;
						lhsAttrType = TypeInt;
					} else {
						memcpy(&f1, (char *)data1+offset1, sizeof(float));
						offset1 += 4;
						lhsAttrType = TypeReal;
					}
				} else {
					if (leftAttrs[i].type == TypeVarChar) {
						int tempLen;
						memcpy(&tempLen, (char *)data1+offset1, sizeof(int));
						offset1 += (4+tempLen);
					} else {
						offset1 += 4;
					}
				}
			}
			void* data2 = malloc(1000);
//			rhsValue = condition.rhsValue;
			void *conditionData = malloc(100);
			if (lhsAttrType == TypeVarChar) {
				int len;
				memcpy(&len, (char *)temp1, sizeof(int));
				memcpy((char *)conditionData, (char *)temp1, len+4);
			} else if (lhsAttrType == TypeReal) {
				memcpy((char *)conditionData, &f1, sizeof(float));
			} else if (lhsAttrType == TypeInt) {
				memcpy((char *)conditionData, &len1, sizeof(int));
			}
			rightIn->setIterator(condition.op, conditionData);
			while (rightIn->getNextTuple(data2) != QE_EOF) {
				int offset2=0;
				int flag = 0;
				for (int j = 0; j < (int)rightAttrs.size(); j++) {
					if (rightAttrs[j].name == condition.rhsAttr) {
						if (rightAttrs[j].type == TypeVarChar) {
							string x;
							string y;
							int tmpLen;
							memcpy(&tmpLen, (char *)data2+offset2, sizeof(int));
							offset2 += 4;
							memcpy(&y, (char *)data2+offset2, tmpLen);
							offset2 += tmpLen;
							memcpy(&x, (char *)temp1, len1);
							switch(condition.op) {
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
						} else if (rightAttrs[j].type == TypeReal) {
							float x;
							float y;
							x = f1;
							memcpy(&y, (char *)data2+offset2, sizeof(float));
							offset2 += 4;
							switch(condition.op) {
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
							int x;
							int y;
							x = len1;
							memcpy(&y, (char *)data2+offset2, sizeof(int));
							offset2 += 4;
							switch(condition.op) {
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
						}
					} else {
						if (rightAttrs[j].type == TypeVarChar) {
							int tempLen;
							memcpy(&tempLen, (char *)data2+offset2, sizeof(int));
							offset2 += (4+tempLen);

						} else {
							offset2 += 4;
						}
					}
				} // end of for
				if (flag == 1) {
					void *temp = malloc(1500);
					memcpy((char *)temp, (char *)data1, offset1);
					memcpy((char *)temp+offset1, (char *)data2, offset2);
					inlJoin.push_back(temp);
					len.push_back(offset1+offset2);
					count++;
				}
			} // end of second iter while
			free(data2);
		}
	}
	free(data1);
}

INLJoin::~INLJoin() {
	for (int i = 0; i < (int)inlJoin.size(); i++) {
		free(inlJoin.at(i));
	}
	inlJoin.clear();
}

RC INLJoin::getNextTuple(void *data) {
	if (iterCount < count) {
		memcpy((char *)data, (char *)(inlJoin.at(iterCount)), len.at(iterCount));
		iterCount++;
		return 0;
	} else {
		return QE_EOF;
	}
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const{
	attrs.clear();
	attrs = this->attrs;
}

HashJoin::HashJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, unsigned int numpages){
	int numPartitions = 10;
	vector<Attribute> leftAttrs;
	leftIn->getAttributes(leftAttrs);
	leftIn->getAttributes(attrs);
	vector<Attribute> rightAttrs;
	rightIn->getAttributes(rightAttrs);
	void *data1 = malloc(1000);
	void *temp1 = malloc(1000);
	int offset1 = 0, attrVal;
	string fileNames[2], tableName;
	Iterator *inArr[2];
	PF_FileHandle fHandles1[numPartitions];
	PF_FileHandle fHandles2[numPartitions];
	PF_Manager *pf = PF_Manager::Instance();
	RM *rm = RM::Instance();
	RID rid;
	int fi;
	vector<Attribute> attrArr[2];
	bool isLeftArg;
	union attrVals attrValue;
	int atrType;

	for (int i = 0; i < (int)rightAttrs.size(); i++) {
		attrs.push_back(rightAttrs[i]);
	}

	count = 0;
	iterCount = 0;
	attrArr[0] = leftAttrs;
	attrArr[1] = rightAttrs;
	fileNames[0] = "LFile";
	fileNames[1] = "RFile";
	inArr[0] = leftIn;
	inArr[1] = rightIn;

	for(int p=0; p<2; p++){
		string tName;
		if(p==0)
			isLeftArg = true;
		else
			isLeftArg = false;
		for(int i=0; i<numPartitions; i++){
			stringstream out1;
			out1 << i;
			tName = fileNames[p];
			rm->createTable(tName.append(out1.str()), attrArr[p]);
		}
		while (inArr[p]->getNextTuple(data1) != QE_EOF) {
			stringstream out;
			offset1 = 0;
			GetAttrVal(data1, attrArr[p], attrValue, offset1, atrType, condition, isLeftArg);
			if(atrType == TypeVarChar){
				int i=1;
				attrVal = (attrValue.s)[0];
				while((attrValue.s)[i]!='\0' && i<3){
					attrVal = attrVal*10+(attrValue.s)[i];
					i++;
				}
				free(attrValue.s);
			}
			else if(atrType == TypeReal){
				attrVal = floor(attrValue.f);
			}
			else {
				attrVal = attrValue.i;
			}
			fi = attrVal%numPartitions;
			out << fi;
			tableName = fileNames[p];
			rm->insertTuple((tableName.append(out.str())).c_str(), data1, rid);
		}
	}
	for(int i=0; i<numPartitions; i++) {
		string tableName1 = fileNames[0];
		string tableName2 = fileNames[1];
		stringstream out;
		vector<void *> tuple1[12];//, tuple2[12];
		vector<int> tupleSize1[12];

		out<<i;
		tableName1.append(out.str());
		tableName2.append(out.str());

		pf->OpenFile(tableName1.c_str(), fHandles1[i]);
		pf->OpenFile(tableName2.c_str(), fHandles2[i]);
		if(fHandles1[i].GetNumberOfPages()==0 || fHandles2[i].GetNumberOfPages()==0){
			pf->CloseFile(fHandles1[i]);
			pf->CloseFile(fHandles2[i]);
			continue;
		}
		else {
			pf->CloseFile(fHandles1[i]);
			pf->CloseFile(fHandles2[i]);
		}
		TableScan *tblScan1 = new TableScan(*rm, tableName1.c_str());
		TableScan *tblScan2 = new TableScan(*rm, tableName2.c_str());
		isLeftArg = true;
		while(tblScan1->getNextTuple(temp1) != QE_EOF){
			offset1 = 0;
			GetAttrVal(temp1, attrArr[0], attrValue, offset1, atrType, condition, isLeftArg);
			if(atrType == TypeVarChar){
				int i=1;
				attrVal = (attrValue.s)[0];
				while((attrValue.s)[i]!='\0' && i<3){
					attrVal = attrVal*10+(attrValue.s)[i];
					i++;
				}
				free(attrValue.s);
			}
			else if(atrType == TypeReal){
				attrVal = floor(attrValue.f);
			}
			else {
				attrVal = attrValue.i;
			}
			fi = attrVal%12;
			tuple1[fi].push_back(temp1);
			tupleSize1[fi].push_back(offset1);
			temp1 = malloc(1000);
		}
		while(tblScan2->getNextTuple(data1) != QE_EOF){
			union attrVals leftValue, rightValue;
			int offset = 0;
//			temp1 = malloc(1500);
			isLeftArg = false;
			offset1 = 0;
			GetAttrVal(data1, attrArr[1], rightValue, offset1, atrType, condition, isLeftArg);
			if(atrType == TypeVarChar){
				int i=1;
				attrVal = (rightValue.s)[0];
				while((rightValue.s)[i]!='\0' && i<3){
					attrVal = attrVal*10+(rightValue.s)[i];
					i++;
				}
			}
			else if(atrType == TypeReal){
				attrVal = floor(rightValue.f);
			}
			else {
				attrVal = rightValue.i;
			}
			fi = attrVal%12;
			bool isJoinSat = false;
			unsigned int t;
			for(t=0; t< tuple1[fi].size(); t++){
				GetAttrVal(tuple1[fi][t], leftAttrs, leftValue, offset, atrType, condition, true);
				if(atrType == TypeVarChar){
					string s1 = leftValue.s;
					string s2 = rightValue.s;
					if(s1 == s2){
						isJoinSat = true;
						break;
					}
					free(rightValue.s);
					free(leftValue.s);
				}
				else if(atrType == TypeReal){
					if(leftValue.f == rightValue.f){
						cout << leftValue.f << endl;
						isJoinSat = true;
						break;
					}
				}
				else {
					if(leftValue.i == rightValue.i){
						isJoinSat = true;
						break;
					}
				}
			}
			if(isJoinSat){
				void *temp = malloc(1500);
				memcpy((char *)temp, (char *)tuple1[fi][t], tupleSize1[fi][t]);
				memcpy((char *)temp+tupleSize1[fi][t], (char *)data1, offset1);
				joinData.push_back(temp);
				joinDataSize.push_back(offset1+tupleSize1[fi][t]);
				count++;
			}
		}
		for(int t=0; t<12; t++){
			for(unsigned int k=0; k<tuple1[t].size(); k++)
				free(tuple1[t][k]);
			tuple1[t].clear();
			tupleSize1[t].clear();
		}
	}
	free(data1);
}

HashJoin::~HashJoin() {
	for (int i = 0; i < (int)joinData.size(); i++) {
		free(joinData.at(i));
	}
	joinData.clear();
}

RC HashJoin::getNextTuple(void *data) {
	if (iterCount < count) {
		memcpy((char *)data, (char *)(joinData.at(iterCount)), joinDataSize.at(iterCount));
		iterCount++;
		return 0;
	} else {
		return QE_EOF;
	}
}

void HashJoin::getAttributes(vector<Attribute> &attrs) const{
	attrs.clear();
	attrs = this->attrs;
}

RC GetAttrVal(const void *data1, vector<Attribute> attr, union attrVals &attrVal, int &offset1, int &atrType, Condition condition, bool isLeftArg){
	int len1;
	string condArg;

	if(isLeftArg)
		condArg = condition.lhsAttr;
	else
		condArg = condition.rhsAttr;

	for (int i = 0; i < (int)attr.size(); i++) {
		if (attr[i].name == condArg) {
			if (attr[i].type == TypeVarChar) {
				memcpy(&len1, (char *)data1+offset1, sizeof(int));
				attrVal.s = (char *) malloc(100);
				memcpy(attrVal.s, (char *)(data1)+offset1+4, len1);
				offset1 += (len1 + 4);
				atrType = TypeVarChar;
			}
			else if(attr[i].type == TypeReal){
				memcpy(&(attrVal.f), (char *)data1+offset1, sizeof(float));
				offset1 += 4;
				atrType = TypeReal;
			}
			else {
				memcpy(&(attrVal.i), (char *)data1+offset1, sizeof(int));
				offset1 += 4;
				atrType = TypeInt;
			}
		}
		else {
			if (attr[i].type == TypeVarChar) {
				int tempLen;
				memcpy(&tempLen, (char *)data1+offset1, sizeof(int));
				offset1 += (4+tempLen);

			} else {
				offset1 += 4;
			}
		}
	}
	return 0;
}
