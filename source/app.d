import std.stdio;
import ceph4d.radosclient;
import ceph4d.rados;
import ceph4d.radostypes;
import std.path;
import std.exception;
import std.string;
import std.array;

void main()
{
	import core.runtime;

	auto list = Runtime.args();
	if(list.length != 2)
	{
		writeln("need file name");
		return ;
	}
	string file = list[1];
	import std.file;
	if(!exists(file))
	{
		writeln("not exist file name");
		return;
	}
	foreach (line; File(file).byLine())
	{
		auto xx = line.strip().split(".");
		writeln(exportFile(cast(string)(line.strip()), cast(string)(xx[0])));
	}
}

string exportFile(string filename, string hash)
{
	RadosClient rados = getRadosClientObj();

	
	//读取的缓存大小
	enum BUFF_SIZE = 1024*1024;
	char[BUFF_SIZE] buf;
	int n=0,size =0;
	
	Appender!string binaryData = appender!string;
	
	do
	{
		n = rados.read(hash, cast(char*)buf, BUFF_SIZE, size);
		
		if(n < 0)
		{
			//error 
			//throw new Exception("error ");
			import std.conv;
			return filename ~" not exist " ~ to!(string)(n);
		}
		binaryData.put(buf[0 .. n]);
		size += n;
	}while(n == BUFF_SIZE);
	import std.file;
	write("./export/" ~ filename, binaryData.data);

	return filename;
}
