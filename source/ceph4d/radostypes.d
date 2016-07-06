module ceph4d.radostypes;
version(linux):
struct obj_watch_t
{
    char [256]addr;
    int int64_t;
    int uint64_t;
    int uint32_t;
}
