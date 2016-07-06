module ceph4d.radosclient;
version (linux)  : 
import ceph4d.radostypes;
import ceph4d.rados;


import std.string : toStringz;
import std.stdio;

/**
*RadosClient
*author donglei xiaosan@outloo.com
*/
class RadosClient
{
    ///string clusterName, string userName
    this(string clusterName, string userName)
    {
        this.clusterName = clusterName;
        this.userName = userName;
    }

    this(string clusterName, string userName, string configPath)
    {
        this(clusterName, userName);
        this.configPath = configPath;
    }

    this(string clusterName, string userName, string configPath, string poolName)
    {
        this(clusterName, userName, configPath);
        this.poolName = poolName;
    }
    ///连接
    void connect()
    {
        //err = rados_create2(&cluster, cast(char*)(clusterName.dup),cast(char*)(userName.dup),flags);
        err = rados_create2( &cluster, clusterName.toStringz,
            userName.toStringz, flags);
        assert(err >= 0, "rados_create2 error");

        /* Read a Ceph configuration file to configure the cluster handle. */
        import std.file;

        if (!isFile(this.configPath))
        {
            assert(0, "config file can not read:" ~ configPath);
        }
        writeln("configPath : ",configPath);
        err = rados_conf_read_file(cluster, configPath.toStringz);
        if (err < 0)
        {
            writeln("config file...");
            writeln(configPath);
            assert(err >= 0, "cannot read config file:" ~ configPath);
        }

        /* Connect to the cluster */
        err = rados_connect(cluster);
        assert(err >= 0, "ccannot connect to cluster: " ~ clusterName);
    }

    ///是否连接
    @property bool isConnected()
    {
        return this.getInstanceId() != 0;
    }

    auto getInstanceId()
    {
        return rados_get_instance_id(cluster);
    }
    ~this()
    {
        this.close();
    }

    ///创建io poolNmae
    void ioCtxCreate(string poolName)
    {
        this.poolName = poolName;

        err = rados_ioctx_create(cluster, poolName.toStringz,  & io);
        assert(err >= 0, "cannot open rados pool: " ~ poolName);
    }

    ///创建io poolNmae
    void ioCtxCreate()
    {

        err = rados_ioctx_create(cluster, poolName.toStringz,  & io);
        assert(err >= 0, "cannot open rados pool: " ~ poolName);
    }

    ///创建io poolNmae
    void write(string key, string value,ulong begin = 0)
    {
       // writeln("write size : ", value.length, "  key is :",key);
        err = rados_write(io, key.toStringz, value.ptr, value.length, begin);
       // writeln("erro ", err);
        if (err < 0)
        {
            assert(0, "write write error ");
        }
    }
    
    void append(string key, string value)
    {
       // writeln("append size : ", value.length, "  key is :",key);
        err = rados_append(io, key.toStringz, value.ptr, value.length);
        //writeln("erro ", err);
        if (err < 0)
        {
            assert(0, "write write error ");
        }
    }
    ///关闭
    void close()
    {
        if (io !is null)
            rados_ioctx_destroy(io);
        if (cluster !is null)
            rados_shutdown(cluster);
    }

    //
    void remove(string key)
    {
        err = rados_remove(io, key.toStringz);

        if (err < 0)
        {
            writeln("Ceph remove key:%s error.", key);
        }
    }
    /**
	* Read data from an object
	* The io context determines the snapshot to read from, if any was set
	* by rados_ioctx_snap_set_read().
	* @param oid the name of the object to read from
	* @param buf where to store the results
	* @param len the number of bytes to read
	* @param off the offset to start reading from in the object
	* @returns number of bytes read on success, negative error code on
	* failure
	*/
    int read(string oid, char * buf, size_t len, uint64_t off = 0)
    {

        //return rados_read(io, cast(const char *)oid,buf,len,off);
        return rados_read(io, oid.toStringz, buf, len, off);
    }

    /**
	* Get the value of an extended attribute on an object.
	* @param o name of the object
	* @param name which extended attribute to read
	* @param buf where to store the result
	* @param len size of buf in bytes
	* @returns length of xattr value on success, negative error code on failure
	*/
    int getXAttr(string oid, string attrName, char * buf, size_t len = 50)
    {
        auto error = rados_getxattr(io, oid.toStringz, attrName.toStringz, buf, len);
        if (error < 0)
        {
            throw new Exception("read xattr is error");
        }

        return error;
    }

    /**
	* Set an extended attribute on an object.
	* @param o name of the object
	* @param name which extended attribute to set
	* @param buf what to store in the xattr
	* @param len the number of bytes in buf
	* @returns 0 on success, negative error code on failure
	*/
    int setXAttr(string oid, string attrName, string buf)
    {
        return rados_setxattr(io, oid.toStringz, attrName.toStringz, buf.toStringz,
            buf.length);
    }

    /**
	* Delete an extended attribute from an object.
	* @param io the context in which to delete the xattr
	* @param o the name of the object
	* @param name which xattr to delete
	* @returns 0 on success, negative error code on failure
	*/
    int rmXAttr(string oid, string attrName)
    {
        return rados_rmxattr(io, oid.toStringz, attrName.toStringz);
    }
    /**
	* free a rados-allocated buffer
	* Release memory allocated by librados calls like rados_mon_command().
	* @param buf buffer pointer
	*/
    void freeBuffer(char * buf)
    {
        rados_buffer_free(buf);
    }
    
private :
    string clusterName;
    rados_t cluster;
    string userName;
    uint64_t flags;
    int err;
    string configPath;

    rados_ioctx_t io;
    string poolName = "data";
}

__gshared RadosClient rados = null;
///获取radosclient obj

RadosClient getRadosClientObj()
{
    if (rados !is null && rados.isConnected)
    {
        return rados;
    }
    //import utils.ini;
    import std.string;

    version(from_config)
    {
	/*[ceph]
	cluster_name = ceph
	user_name = client.admin
	config_path = /etc/ceph/ceph.conf
	pool_name = rbd*/
    auto clusterName = iniInstance.value("ceph", "cluster_name").strip;
    auto userName = iniInstance.value("ceph", "user_name").strip;
    auto configPath = iniInstance.value("ceph", "config_path").strip;
    auto poolName = iniInstance.value("ceph", "pool_name").strip; 
    }
	else
	{
		auto clusterName = "ceph";
		auto userName = "client.admin";
		auto configPath = "/etc/ceph/ceph.conf";
		auto poolName = "rbd"; 
	}

    //import std.stdio,std.string;
    //writeln(format("clusterName:%s:userName:%s:configPath:%s:poolName:%s:", clusterName, userName,configPath, poolName));
    rados = new RadosClient(clusterName, userName, configPath, poolName);

    rados.connect();
    rados.ioCtxCreate();
    return rados;
}

unittest
{
    import ceph4d.radosclient;

    RadosClient rados = getRadosClientObj();
    rados.connect();
    rados.ioCtxCreate();
    rados.write("key", "v");
}

