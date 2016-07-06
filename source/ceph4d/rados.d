module ceph4d.rados;
version (linux)  :  /*
* Ceph - scalable distributed file system
*
* Copyright (C) 2004-2012 Sage Weil <sage@newdream.net>
*
* This is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License version 2.1, as published by the Free Software
* Foundation.  See file COPYING.
*
*/
/*
* Flags that can be set on a per-op basis via
* rados_read_op_set_flags() and rados_write_op_set_flags().
*/
enum
{
    LIBRADOS_OP_FLAG_EXCL = 1,
    LIBRADOS_OP_FLAG_FAILOK,
    LIBRADOS_OP_FLAG_FADVISE_RANDOM = 4,
    LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL = 8,
    LIBRADOS_OP_FLAG_FADVISE_WILLNEED = 16,
    LIBRADOS_OP_FLAG_FADVISE_DONTNEED = 32,
    LIBRADOS_OP_FLAG_FADVISE_NOCACHE = 64,
}

/**
* @defgroup librados_h_xattr_comp xattr comparison operations
* Operators for comparing xattrs on objects, and aborting the
* rados_read_op or rados_write_op transaction if the comparison
* fails.
*
* @{
*/
enum
{
    LIBRADOS_CMPXATTR_OP_EQ = 1,
    LIBRADOS_CMPXATTR_OP_NE,
    LIBRADOS_CMPXATTR_OP_GT,
    LIBRADOS_CMPXATTR_OP_GTE,
    LIBRADOS_CMPXATTR_OP_LT,
    LIBRADOS_CMPXATTR_OP_LTE,
}
/** @} */

/**
* @defgroup librados_h_operation_flags Operation Flags
* Flags for rados_read_op_opeprate(), rados_write_op_operate(),
* rados_aio_read_op_operate(), and rados_aio_write_op_operate().
* See librados.hpp for details.
* @{
*/
enum
{
    LIBRADOS_OPERATION_NOFLAG,
    LIBRADOS_OPERATION_BALANCE_READS,
    LIBRADOS_OPERATION_LOCALIZE_READS,
    LIBRADOS_OPERATION_ORDER_READS_WRITES = 4,
    LIBRADOS_OPERATION_IGNORE_CACHE = 8,
    LIBRADOS_OPERATION_SKIPRWLOCKS = 16,
    LIBRADOS_OPERATION_IGNORE_OVERLAY = 32,
}
/** @} */

/*
* snap id contants
*/

extern (C) :  /**
nothrow:
@nogc:

* @typedef rados_t
*
* A handle for interacting with a RADOS cluster. It encapsulates all
* RADOS client configuration, including username, key for
* authentication, logging, and debugging. Talking different clusters
* -- or to the same cluster with different users -- requires
* different cluster handles.
*/
alias rados_t = void *;

/**
* @tyepdef rados_config_t
*
* A handle for the ceph configuration context for the rados_t cluster
* instance.  This can be used to share configuration context/state
* (e.g., logging configuration) between librados instance.
*
* @warning The config context does not have independent reference
* counting.  As such, a rados_config_t handle retrieved from a given
* rados_t is only valid as long as that rados_t.
*/

alias  rados_config_t = void *;

/**
* @typedef rados_ioctx_t
*
* An io context encapsulates a few settings for all I/O operations
* done on it:
* - pool - set when the io context is created (see rados_ioctx_create())
* - snapshot context for writes (see
*   rados_ioctx_selfmanaged_snap_set_write_ctx())
* - snapshot id to read from (see rados_ioctx_snap_set_read())
* - object locator for all single-object operations (see
*   rados_ioctx_locator_set_key())
* - namespace for all single-object operations (see
*   rados_ioctx_set_namespace()).  Set to LIBRADOS_ALL_NSPACES
*   before rados_nobjects_list_open() will list all objects in all
*   namespaces.
*
* @warning Changing any of these settings is not thread-safe -
* librados users must synchronize any of these changes on their own,
* or use separate io contexts for each thread
*/
alias  rados_ioctx_t = void *;

/**
* @typedef rados_list_ctx_t
*
* An iterator for listing the objects in a pool.
* Used with rados_nobjects_list_open(),
* rados_nobjects_list_next(), and
* rados_nobjects_list_close().
*/
//C     typedef void *rados_list_ctx_t;
alias  rados_list_ctx_t = void *;

/**
* @typedef rados_snap_t
* The id of a snapshot.
*/
//C     typedef uint64_t rados_snap_t;
alias  uint64_t = ulong;

alias  uint8_t = ubyte;

/**
* @typedef rados_xattrs_iter_t
* An iterator for listing extended attrbutes on an object.
* Used with rados_getxattrs(), rados_getxattrs_next(), and
* rados_getxattrs_end().
*/
//C     typedef void *rados_xattrs_iter_t;
alias rados_xattrs_iter_t = void * ;

/**
* @typedef rados_omap_iter_t
* An iterator for listing omap key/value pairs on an object.
* Used with rados_read_op_omap_get_keys(), rados_read_op_omap_get_vals(),
* rados_read_op_omap_get_vals_by_keys(), rados_omap_get_next(), and
* rados_omap_get_end().
*/
alias rados_omap_iter_t = void * ;

/**
* @struct rados_pool_stat_t
* Usage information for a pool.
*/
struct rados_pool_stat_t
{
    uint64_t num_bytes;
    uint64_t num_kb;
    uint64_t num_objects;
    uint64_t num_object_clones;
    uint64_t num_object_copies;
    uint64_t num_objects_missing_on_primary;
    uint64_t num_objects_unfound;
    uint64_t num_objects_degraded;
    uint64_t num_rd;
    uint64_t num_rd_kb;
    uint64_t num_wr;
    uint64_t num_wr_kb;
}

/**
* @struct rados_cluster_stat_t
* Cluster-wide usage information
*/
struct rados_cluster_stat_t
{
    uint64_t kb;
    uint64_t kb_used;
    uint64_t kb_avail;
    uint64_t num_objects;
}

/**
* @typedef rados_write_op_t
*
* An object write operation stores a number of operations which can be
* executed atomically. For usage, see:
* - Creation and deletion: rados_create_write_op() rados_release_write_op()
* - Extended attribute manipulation: rados_write_op_cmpxattr()
*   rados_write_op_cmpxattr(), rados_write_op_setxattr(),
*   rados_write_op_rmxattr()
* - Object map key/value pairs: rados_write_op_omap_set(),
*   rados_write_op_omap_rm_keys(), rados_write_op_omap_clear(),
*   rados_write_op_omap_cmp()
* - Object properties: rados_write_op_assert_exists(),
*   rados_write_op_assert_version()
* - Creating objects: rados_write_op_create()
* - IO on objects: rados_write_op_append(), rados_write_op_write(), rados_write_op_zero
*   rados_write_op_write_full(), rados_write_op_remove, rados_write_op_truncate(),
*   rados_write_op_zero()
* - Hints: rados_write_op_set_alloc_hint()
* - Performing the operation: rados_write_op_operate(), rados_aio_write_op_operate()
*/
alias  rados_write_op_t = void *;

/**
* @typedef rados_read_op_t
*
* An object read operation stores a number of operations which can be
* executed atomically. For usage, see:
* - Creation and deletion: rados_create_read_op() rados_release_read_op()
* - Extended attribute manipulation: rados_read_op_cmpxattr(),
*   rados_read_op_getxattr(), rados_read_op_getxattrs()
* - Object map key/value pairs: rados_read_op_omap_get_vals(),
*   rados_read_op_omap_get_keys(), rados_read_op_omap_get_vals_by_keys(),
*   rados_read_op_omap_cmp()
* - Object properties: rados_read_op_stat(), rados_read_op_assert_exists(),
*   rados_read_op_assert_version()
* - IO on objects: rados_read_op_read()
* - Custom operations: rados_read_op_exec(), rados_read_op_exec_user_buf()
* - Request properties: rados_read_op_set_flags()
* - Performing the operation: rados_read_op_operate(),
*   rados_aio_read_op_operate()
*/
alias  rados_read_op_t = void *;

/**
* Get the version of librados.
*
* The version number is major.minor.extra. Note that this is
* unrelated to the Ceph version number.
*
* TODO: define version semantics, i.e.:
* - incrementing major is for backwards-incompatible changes
* - incrementing minor is for backwards-compatible changes
* - incrementing extra is for bug fixes
*
* @param major where to store the major version number
* @param minor where to store the minor version number
* @param extra where to store the extra version number
*/

void rados_version(int * major, int * minor, int * extra);

/**
* @defgroup librados_h_init Setup and Teardown
* These are the first and last functions to that should be called
* when using librados.
*
* @{
*/

/**
* Create a handle for communicating with a RADOS cluster.
*
* Ceph environment variables are read when this is called, so if
* $CEPH_ARGS specifies everything you need to connect, no further
* configuration is necessary.
*
* @param cluster where to store the handle
* @param id the user to connect as (i.e. admin, not client.admin)
* @returns 0 on success, negative error code on failure
*/
//C     CEPH_RADOS_API int rados_create(rados_t *cluster, const char * const id);
int rados_create(rados_t * cluster, char * id);

/**
* Extended version of rados_create.
*
* Like rados_create, but 
* 1) don't assume 'client\.'+id; allow full specification of name
* 2) allow specification of cluster name
* 3) flags for future expansion
*/
//C     CEPH_RADOS_API int rados_create2(rados_t *pcluster,
//C                                      const char *const clustername,
//C                                      const char * const name, uint64_t flags);
int rados_create2(rados_t * pcluster,const char * clustername,const char * name, uint64_t flags);

/**
* Initialize a cluster handle from an existing configuration.
*
* Share configuration state with another rados_t instance.
*
* @param cluster where to store the handle
* @param cct the existing configuration to use
* @returns 0 on success, negative error code on failure
*/
//C     CEPH_RADOS_API int rados_create_with_context(rados_t *cluster,
//C                                                  rados_config_t cct);
int rados_create_with_context(rados_t * cluster, rados_config_t cct);

/**
* Ping the monitor with ID mon_id, storing the resulting reply in
* buf (if specified) with a maximum size of len.
*
* The result buffer is allocated on the heap; the caller is
* expected to release that memory with rados_buffer_free().  The
* buffer and length pointers can be NULL, in which case they are
* not filled in.
*
* @param      cluster    cluster handle
* @param[in]  mon_id     ID of the monitor to ping
* @param[out] outstr     double pointer with the resulting reply
* @param[out] outstrlen  pointer with the size of the reply in outstr
*/
//C     CEPH_RADOS_API int rados_ping_monitor(rados_t cluster, const char *mon_id,
//C                                           char **outstr, size_t *outstrlen);
int rados_ping_monitor(rados_t cluster,const char * mon_id, char *  * outstr, size_t * outstrlen);

/**
* Connect to the cluster.
*
* @note BUG: Before calling this, calling a function that communicates with the
* cluster will crash.
*
* @pre The cluster handle is configured with at least a monitor
* address. If cephx is enabled, a client name and secret must also be
* set.
*
* @post If this succeeds, any function in librados may be used
*
* @param cluster The cluster to connect to.
* @returns 0 on sucess, negative error code on failure
*/
//C     CEPH_RADOS_API int rados_connect(rados_t cluster);
int rados_connect(rados_t cluster);

/**
* Disconnects from the cluster.
* For clean up, this is only necessary after rados_connect() has
* succeeded.
* @warning This does not guarantee any asynchronous writes have
* completed. To do that, you must call rados_aio_flush() on all open
* io contexts.
* @warning We implicitly call rados_watch_flush() on shutdown.  If
* there are watches being used, this should be done explicitly before
* destroying the relevant IoCtx.  We do it here as a safety measure.
* @post the cluster handle cannot be used again
* @param cluster the cluster to shutdown
*/
//C     CEPH_RADOS_API void rados_shutdown(rados_t cluster);
void rados_shutdown(rados_t cluster);

/** @} init */

/**
* @defgroup librados_h_config Configuration
* These functions read and update Ceph configuration for a cluster
* handle. Any configuration changes must be done before connecting to
* the cluster.
*
* Options that librados users might want to set include:
* - mon_host
* - auth_supported
* - key, keyfile, or keyring when using cephx
* - log_file, log_to_stderr, err_to_stderr, and log_to_syslog
* - debug_rados, debug_objecter, debug_monc, debug_auth, or debug_ms
*
* All possible options can be found in src/common/config_opts.h in ceph.git
*
* @{
*/

/**
* Configure the cluster handle using a Ceph config file
*
* If path is NULL, the default locations are searched, and the first
* found is used. The locations are:
* - $CEPH_CONF (environment variable)
* - /etc/ceph/ceph.conf
* - ~/.ceph/config
* - ceph.conf (in the current working directory)
*
* @pre rados_connect() has not been called on the cluster handle
*
* @param cluster cluster handle to configure
* @param path path to a Ceph configuration file
* @returns 0 on success, negative error code on failure
*/
//C     CEPH_RADOS_API int rados_conf_read_file(rados_t cluster, const char *path);
int rados_conf_read_file(rados_t cluster,const char * path);

/**
* Configure the cluster handle with command line arguments
*
* argv can contain any common Ceph command line option, including any
* configuration parameter prefixed by '--' and replacing spaces with
* dashes or underscores. For example, the following options are equivalent:
* - --mon-host 10.0.0.1:6789
* - --mon_host 10.0.0.1:6789
* - -m 10.0.0.1:6789
*
* @pre rados_connect() has not been called on the cluster handle
*
* @param cluster cluster handle to configure
* @param argc number of arguments in argv
* @param argv arguments to parse
* @returns 0 on success, negative error code on failure
*/
//C     CEPH_RADOS_API int rados_conf_parse_argv(rados_t cluster, int argc,
//C                                              const char **argv);
int rados_conf_parse_argv(rados_t cluster, int argc,const char *  * argv);

/**
* Configure the cluster handle with command line arguments, returning
* any remainders.  Same rados_conf_parse_argv, except for extra
* remargv argument to hold returns unrecognized arguments.
*
* @pre rados_connect() has not been called on the cluster handle
*
* @param cluster cluster handle to configure
* @param argc number of arguments in argv
* @param argv arguments to parse
* @param remargv char* array for returned unrecognized arguments
* @returns 0 on success, negative error code on failure
*/
//C     CEPH_RADOS_API int rados_conf_parse_argv_remainder(rados_t cluster, int argc,
//C     				                   const char **argv,
//C                                                        const char **remargv);
int rados_conf_parse_argv_remainder(rados_t cluster, int argc,const char *  * argv,const char *  * remargv);
/**
* Configure the cluster handle based on an environment variable
*
* The contents of the environment variable are parsed as if they were
* Ceph command line options. If var is NULL, the CEPH_ARGS
* environment variable is used.
*
* @pre rados_connect() has not been called on the cluster handle
*
* @note BUG: this is not threadsafe - it uses a static buffer
*
* @param cluster cluster handle to configure
* @param var name of the environment variable to read
* @returns 0 on success, negative error code on failure
*/
//C     CEPH_RADOS_API int rados_conf_parse_env(rados_t cluster, const char *var);
int rados_conf_parse_env(rados_t cluster,const char * var);

/**
* Set a configuration option
*
* @pre rados_connect() has not been called on the cluster handle
*
* @param cluster cluster handle to configure
* @param option option to set
* @param value value of the option
* @returns 0 on success, negative error code on failure
* @returns -ENOENT when the option is not a Ceph configuration option
*/
//C     CEPH_RADOS_API int rados_conf_set(rados_t cluster, const char *option,
//C                                       const char *value);
int rados_conf_set(rados_t cluster,const char * option,const char * value);

/**
* Get the value of a configuration option
*
* @param cluster configuration to read
* @param option which option to read
* @param buf where to write the configuration value
* @param len the size of buf in bytes
* @returns 0 on success, negative error code on failure
* @returns -ENAMETOOLONG if the buffer is too short to contain the
* requested value
*/
//C     CEPH_RADOS_API int rados_conf_get(rados_t cluster, const char *option,
//C                                       char *buf, size_t len);
int rados_conf_get(rados_t cluster,const char * option, char * buf, size_t len);

/** @} config */

/**
* Read usage info about the cluster
*
* This tells you total space, space used, space available, and number
* of objects. These are not updated immediately when data is written,
* they are eventually consistent.
*
* @param cluster cluster to query
* @param result where to store the results
* @returns 0 on success, negative error code on failure
*/
//C     CEPH_RADOS_API int rados_cluster_stat(rados_t cluster,
//C                                           struct rados_cluster_stat_t *result);
int rados_cluster_stat(rados_t cluster, rados_cluster_stat_t * result);

/**
* Get the fsid of the cluster as a hexadecimal string.
*
* The fsid is a unique id of an entire Ceph cluster.
*
* @param cluster where to get the fsid
* @param buf where to write the fsid
* @param len the size of buf in bytes (should be 37)
* @returns 0 on success, negative error code on failure
* @returns -ERANGE if the buffer is too short to contain the
* fsid
*/
//C     CEPH_RADOS_API int rados_cluster_fsid(rados_t cluster, char *buf, size_t len);
int rados_cluster_fsid(rados_t cluster,const char * buf, size_t len);

/**
* Get/wait for the most recent osdmap
* 
* @param cluster the cluster to shutdown
* @returns 0 on sucess, negative error code on failure
*/
//C     CEPH_RADOS_API int rados_wait_for_latest_osdmap(rados_t cluster);
int rados_wait_for_latest_osdmap(rados_t cluster);

/**
* @defgroup librados_h_pools Pools
*
* RADOS pools are separate namespaces for objects. Pools may have
* different crush rules associated with them, so they could have
* differing replication levels or placement strategies. RADOS
* permissions are also tied to pools - users can have different read,
* write, and execute permissions on a per-pool basis.
*
* @{
*/

/**
* List pools
*
* Gets a list of pool names as NULL-terminated strings.  The pool
* names will be placed in the supplied buffer one after another.
* After the last pool name, there will be two 0 bytes in a row.
*
* If len is too short to fit all the pool name entries we need, we will fill
* as much as we can.
*
* @param cluster cluster handle
* @param buf output buffer
* @param len output buffer length
* @returns length of the buffer we would need to list all pools
*/
//C     CEPH_RADOS_API int rados_pool_list(rados_t cluster, char *buf, size_t len);
int rados_pool_list(rados_t cluster,const char * buf,size_t len);

/**
* Get a configuration handle for a rados cluster handle
*
* This handle is valid only as long as the cluster handle is valid.
*
* @param cluster cluster handle
* @returns config handle for this cluster
*/
//C     CEPH_RADOS_API rados_config_t rados_cct(rados_t cluster);
rados_config_t rados_cct(rados_t cluster);

/**
* Get a global id for current instance
*
* This id is a unique representation of current connection to the cluster
*
* @param cluster cluster handle
* @returns instance global id
*/
//C     CEPH_RADOS_API uint64_t rados_get_instance_id(rados_t cluster);
uint64_t rados_get_instance_id(rados_t cluster);

/**
* Create an io context
*
* The io context allows you to perform operations within a particular
* pool. For more details see rados_ioctx_t.
*
* @param cluster which cluster the pool is in
* @param pool_name name of the pool
* @param ioctx where to store the io context
* @returns 0 on success, negative error code on failure
*/
//C     CEPH_RADOS_API int rados_ioctx_create(rados_t cluster, const char *pool_name,
//C                                           rados_ioctx_t *ioctx);
int rados_ioctx_create(rados_t cluster,const char * pool_name, rados_ioctx_t * ioctx);
//C     CEPH_RADOS_API int rados_ioctx_create2(rados_t cluster, int64_t pool_id,
//C                                            rados_ioctx_t *ioctx);
//int  rados_ioctx_create2(rados_t cluster,int64_t pool_id, rados_ioctx_t *ioctx);
alias int64_t = long;
int  rados_ioctx_create2(rados_t cluster,int64_t pool_id, rados_ioctx_t *ioctx);

/**
* The opposite of rados_ioctx_create
* This just tells librados that you no longer need to use the io context.
* It may not be freed immediately if there are pending asynchronous
* requests on it, but you should not use an io context again after
* calling this function on it.
* @warning This does not guarantee any asynchronous
* writes have completed. You must call rados_aio_flush()
* on the io context before destroying it to do that.
* @warning If this ioctx is used by rados_watch, the caller needs to
* be sure that all registered watches are disconnected via
* rados_unwatch() and that rados_watch_flush() is called.  This
* ensures that a racing watch callback does not make use of a
* destroyed ioctx.
* @param io the io context to dispose of
*/
//C     CEPH_RADOS_API void rados_ioctx_destroy(rados_ioctx_t io);
void rados_ioctx_destroy(rados_ioctx_t io);

/**
* Get configuration hadnle for a pool handle
*
* @param io pool handle
* @returns rados_config_t for this cluster
*/
//C     CEPH_RADOS_API rados_config_t rados_ioctx_cct(rados_ioctx_t io);
rados_config_t rados_ioctx_cct(rados_ioctx_t io);

/**
* Get the cluster handle used by this rados_ioctx_t
* Note that this is a weak reference, and should not
* be destroyed via rados_shutdown().
*
* @param io the io context
* @returns the cluster handle for this io context
*/
//C     CEPH_RADOS_API rados_t rados_ioctx_get_cluster(rados_ioctx_t io);
rados_t rados_ioctx_get_cluster(rados_ioctx_t io);

/**
* Get pool usage statistics
*
* Fills in a rados_pool_stat_t after querying the cluster.
*
* @param io determines which pool to query
* @param stats where to store the results
* @returns 0 on success, negative error code on failure
*/
//C     CEPH_RADOS_API int rados_ioctx_pool_stat(rados_ioctx_t io,
//C                                              struct rados_pool_stat_t *stats);
int rados_ioctx_pool_stat(rados_ioctx_t io, rados_pool_stat_t * stats);

/**
* Create a pool with default settings
*
* The default owner is the admin user (auid 0).
* The default crush rule is rule 0.
*
* @param cluster the cluster in which the pool will be created
* @param pool_name the name of the new pool
* @returns 0 on success, negative error code on failure
*/
//C     CEPH_RADOS_API int rados_pool_create(rados_t cluster, const char *pool_name);
int rados_pool_create(rados_t cluster, const char * pool_name);

/**
* Create a pool owned by a specific auid
*
* The auid is the authenticated user id to give ownership of the pool.
* TODO: document auid and the rest of the auth system
*
* @param cluster the cluster in which the pool will be created
* @param pool_name the name of the new pool
* @param auid the id of the owner of the new pool
* @returns 0 on success, negative error code on failure
*/
//C     CEPH_RADOS_API int rados_pool_create_with_auid(rados_t cluster,const char *pool_name,uint64_t auid);
int rados_pool_create_with_auid(rados_t cluster, const char * pool_name, uint64_t auid);

/**
* Create a pool with a specific CRUSH rule
*
* @param cluster the cluster in which the pool will be created
* @param pool_name the name of the new pool
* @param crush_rule_num which rule to use for placement in the new pool1
* @returns 0 on success, negative error code on failure
*/
int rados_pool_create_with_crush_rule(rados_t cluster, const char * pool_name,
    uint8_t crush_rule_num);

/**
* Write *len* bytes from *buf* into the *oid* object, starting at
* offset *off*. The value of *len* must be <= UINT_MAX/2.
* @note This will never return a positive value not equal to len.
* @param io the io context in which the write will occur
* @param oid name of the object
* @param buf data to write
* @param len length of the data, in bytes
* @param off byte offset in the object to begin writing at
* @returns 0 on success, negative error code on failure
*/
int rados_write(rados_ioctx_t io, const char * oid, const char * buf, size_t len, uint64_t off);


/**
 * Write *len* bytes from *buf* into the *oid* object. The value of
 * *len* must be <= UINT_MAX/2.
 *
 * The object is filled with the provided data. If the object exists,
 * it is atomically truncated and then written.
 *
 * @param io the io context in which the write will occur
 * @param oid name of the object
 * @param buf data to write
 * @param len length of the data, in bytes
 * @returns 0 on success, negative error code on failure
 */
 int rados_write_full(rados_ioctx_t io, const char *oid,
                                    const char *buf, size_t len);

/**
 * Write the same *data_len* bytes from *buf* multiple times into the
 * *oid* object. *write_len* bytes are written in total, which must be
 * a multiple of *data_len*. The value of *write_len* and *data_len*
 * must be <= UINT_MAX/2.
 *
 * @param io the io context in which the write will occur
 * @param oid name of the object
 * @param buf data to write
 * @param data_len length of the data, in bytes
 * @param write_len the total number of bytes to write
 * @param off byte offset in the object to begin writing at
 * @returns 0 on success, negative error code on failure
 */
 int rados_writesame(rados_ioctx_t io, const char *oid,
                                   const char *buf, size_t data_len,
                                   size_t write_len, uint64_t off);

/**
 * Efficiently copy a portion of one object to another
 *
 * If the underlying filesystem on the OSD supports it, this will be a
 * copy-on-write clone.
 *
 * The src and dest objects must be in the same pg. To ensure this,
 * the io context should have a locator key set (see
 * rados_ioctx_locator_set_key()).
 *
 * @param io the context in which the data is cloned
 * @param dst the name of the destination object
 * @param dst_off the offset within the destination object (in bytes)
 * @param src the name of the source object
 * @param src_off the offset within the source object (in bytes)
 * @param len how much data to copy
 * @returns 0 on success, negative error code on failure
 */
 int rados_clone_range(rados_ioctx_t io, const char *dst,
                                     uint64_t dst_off, const char *src,
                                     uint64_t src_off, size_t len);

/**
 * Append *len* bytes from *buf* into the *oid* object. The value of
 * *len* must be <= UINT_MAX/2.
 *
 * @param io the context to operate in
 * @param oid the name of the object
 * @param buf the data to append
 * @param len length of buf (in bytes)
 * @returns 0 on success, negative error code on failure
 */
 int rados_append(rados_ioctx_t io, const char *oid,
                                const char *buf, size_t len);


alias  rados_completion_t = void *;
/**
* Asychronously read data from an object
* The io context determines the snapshot to read from, if any was set
* by rados_ioctx_snap_set_read().
* The return value of the completion will be number of bytes read on
* success, negative error code on failure.
* @note only the 'complete' callback of the completion will be called.
* @param io the context in which to perform the read
* @param oid the name of the object to read from
* @param completion what to do when the read is complete
* @param buf where to store the results
* @param len the number of bytes to read
* @param off the offset to start reading from in the object
* @returns 0 on success, negative error code on failure
*/
int rados_aio_read(rados_ioctx_t io, const char * oid,
    rados_completion_t completion, char * buf, size_t len, uint64_t off);

/**
* Release a completion
* Call this when you no longer need the completion. It may not be
* freed immediately if the operation is not acked and committed.
* @param c completion to release
*/
void rados_aio_release(rados_completion_t c);
/**
* Block until an operation completes
* This means it is in memory on all replicas.
* @note BUG: this should be void
* @param c operation to wait for
* @returns 0
*/
int rados_aio_wait_for_complete(rados_completion_t c);

/**
* @typedef rados_callback_t
* Callbacks for asynchrous operations take two parameters:
* - cb the completion that has finished
* - arg application defined data made available to the callback function
*/
alias rados_callback_t = void function(rados_completion_t cb, void * arg) ;

/**
* Constructs a completion to use with asynchronous operations
*
* The complete and safe callbacks correspond to operations being
* acked and committed, respectively. The callbacks are called in
* order of receipt, so the safe callback may be triggered before the
* complete callback, and vice versa. This is affected by journalling
* on the OSDs.
*
* TODO: more complete documentation of this elsewhere (in the RADOS docs?)
*
* @note Read operations only get a complete callback.
* @note BUG: this should check for ENOMEM instead of throwing an exception
*
* @param cb_arg application-defined data passed to the callback functions
* @param cb_complete the function to be called when the operation is
* in memory on all relpicas
* @param cb_safe the function to be called when the operation is on
* stable storage on all replicas
* @param pc where to store the completion
* @returns 0
*/
int rados_aio_create_completion(void * cb_arg, rados_callback_t cb_complete,
    rados_callback_t cb_safe, rados_completion_t * pc);

/**
* Delete an object
* @note This does not delete any snapshots of the object.
* @param io the pool to delete the object from
* @param oid the name of the object to delete
* @returns 0 on success, negative error code on failure
*/
int rados_remove(rados_ioctx_t io, const char * oid);

/**
* Read data from an object
* The io context determines the snapshot to read from, if any was set
* by rados_ioctx_snap_set_read().
* @param io the context in which to perform the read
* @param oid the name of the object to read from
* @param buf where to store the results
* @param len the number of bytes to read
* @param off the offset to start reading from in the object
* @returns number of bytes read on success, negative error code on
* failure
*/
int rados_read(rados_ioctx_t io, const char * oid, char * buf, size_t len, uint64_t off);

/**
* @defgroup librados_h_xattrs Xattrs
* Extended attributes are stored as extended attributes on the files
* representing an object on the OSDs. Thus, they have the same
* limitations as the underlying filesystem. On ext4, this means that
* the total data stored in xattrs cannot exceed 4KB.
*
* @{
*/

/**
* Get the value of an extended attribute on an object.
* @param io the context in which the attribute is read
* @param o name of the object
* @param name which extended attribute to read
* @param buf where to store the result
* @param len size of buf in bytes
* @returns length of xattr value on success, negative error code on failure
*/
int rados_getxattr(rados_ioctx_t io, const char * o, const char * name, char * buf,
    size_t len);

/**
* Set an extended attribute on an object.
* @param io the context in which xattr is set
* @param o name of the object
* @param name which extended attribute to set
* @param buf what to store in the xattr
* @param len the number of bytes in buf
* @returns 0 on success, negative error code on failure
*/
int rados_setxattr(rados_ioctx_t io, const char * o, const char * name, const char * buf,
    size_t len);

/**
* Delete an extended attribute from an object.
* @param io the context in which to delete the xattr
* @param o the name of the object
* @param name which xattr to delete
* @returns 0 on success, negative error code on failure
*/
int rados_rmxattr(rados_ioctx_t io, const char * o, const char * name);

/**
* Start iterating over xattrs on an object.
* @post iter is a valid iterator
* @param io the context in which to list xattrs
* @param oid name of the object
* @param iter where to store the iterator
* @returns 0 on success, negative error code on failure
*/
int rados_getxattrs(rados_ioctx_t io, const char * oid, rados_xattrs_iter_t * iter);

/**
* Get the next xattr on the object
* @pre iter is a valid iterator
* @post name is the NULL-terminated name of the next xattr, and val
* contains the value of the xattr, which is of length len. If the end
* of the list has been reached, name and val are NULL, and len is 0.
* @param iter iterator to advance
* @param name where to store the name of the next xattr
* @param val where to store the value of the next xattr
* @param len the number of bytes in val
* @returns 0 on success, negative error code on failure
*/
int rados_getxattrs_next(rados_xattrs_iter_t iter, const char *  * name,
    const char *  * val, size_t * len);

/**
* Close the xattr iterator.
* iter should not be used after this is called.
* @param iter the iterator to close
*/
void rados_getxattrs_end(rados_xattrs_iter_t iter);

/** @} Xattrs */

/**
 * free a rados-allocated buffer
 * Release memory allocated by librados calls like rados_mon_command().
 * @param buf buffer pointer
 */
void rados_buffer_free(char * buf);
