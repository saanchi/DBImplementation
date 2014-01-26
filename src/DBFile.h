#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

typedef enum {heap, sorted, tree} fType;

// Generic DBFile class ,All type of DBFile will inherit this class

class DBFile {
	public:
	DBFile *db_file;
	DBFile (); 
	Buffer MyBuffer;
	virtual ~DBFile();
	virtual int Create (char *fpath, fType file_type, void *startup);
	virtual int Open (char *fpath);
	virtual int Close ();
	virtual void Load (Schema &myschema, char *loadpath);
	virtual void MoveFirst ();
	virtual void Add (Record &addme);
	virtual int GetNext (Record &fetchme);
	virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};


//HeapFile class which inherit  DBFile Class 
class HeapFile:public DBFile{
	File myFile;
	int curPosition;
	int totalCount;	
	public:
	HeapFile ();
	~HeapFile ();
	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();
	void Load (Schema &f_schema, char *loadpath);
	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);		
};

#endif