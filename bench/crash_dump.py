"""Read only diagnostic metadata/code addresses from our PostgreSQL minidump.

Never print raw memory, environment or SQL/data strings. Stack entries below are
candidate code pointers, not an unwound call stack. No debugger installation or
global Windows settings are changed.
"""
import argparse
import bisect
from pathlib import Path
import re
import struct
from bench.common import write_report


def pe_exports(path):
    data=Path(path).read_bytes()
    if data[:2]!=b'MZ':return [],[]
    pe=struct.unpack_from('<I',data,0x3c)[0]
    if data[pe:pe+4]!=b'PE\0\0':return [],[]
    sections,optional=struct.unpack_from('<H',data,pe+6)[0],struct.unpack_from('<H',data,pe+20)[0]
    opt=pe+24
    dd=opt+(112 if struct.unpack_from('<H',data,opt)[0]==0x20b else 96)
    export,size=struct.unpack_from('<II',data,dd)
    maps=[];executable=[]
    for n in range(sections):
        pos=opt+optional+40*n
        virtual_size,rva,raw_size,offset=struct.unpack_from('<IIII',data,pos+8)
        flags=struct.unpack_from('<I',data,pos+36)[0]
        maps.append((rva,max(virtual_size,raw_size),offset))
        if flags&0x20000000:executable.append((rva,rva+virtual_size))
    def file_offset(rva):
        for base,length,offset in maps:
            if base<=rva<base+length:return offset+rva-base
        raise ValueError('RVA not in a section')
    if not export:return [],executable
    e=file_offset(export)
    functions,names,addresses,name_addresses,ordinals=struct.unpack_from('<IIIII',data,e+20)
    results=[]
    for n in range(names):
        name_rva=struct.unpack_from('<I',data,file_offset(name_addresses)+4*n)[0]
        ordinal=struct.unpack_from('<H',data,file_offset(ordinals)+2*n)[0]
        function=struct.unpack_from('<I',data,file_offset(addresses)+4*ordinal)[0]
        start=file_offset(name_rva);end=data.find(b'\0',start)
        name=data[start:end].decode('ascii',errors='replace')
        if not export<=function<export+size:results.append((function,name))
    return sorted(results),executable


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('dump')
    p.add_argument('--output',default='reports/native-unexpected-crash-dump.json')
    args=p.parse_args()
    path=Path(args.dump)
    with path.open('rb') as f:
        header=f.read(32)
        signature,version,count,directory,checksum,timestamp,flags=struct.unpack('<4sIIIIIQ',header)
        if signature!=b'MDMP':raise RuntimeError('Not a Microsoft minidump')
        streams={}
        for n in range(count):
            f.seek(directory+12*n);kind,size,rva=struct.unpack('<III',f.read(12));streams[kind]=(rva,size)
        def at(offset,size):f.seek(offset);return f.read(size)
        ranges=[]
        if 9 in streams:
            pos,_=streams[9];n,base=struct.unpack('<QQ',at(pos,16));cursor=base
            for index in range(n):
                start,length=struct.unpack('<QQ',at(pos+16+16*index,16));ranges.append((start,length,cursor));cursor+=length
        elif 5 in streams:
            pos,_=streams[5];n=struct.unpack('<I',at(pos,4))[0]
            for index in range(n):ranges.append(struct.unpack('<QII',at(pos+4+16*index,16)))
        def memory(address,size):
            for start,length,rva in ranges:
                if start<=address and address+size<=start+length:return at(rva+address-start,size)
            raise KeyError('Requested diagnostic address is not captured')
        def pointer(address):return struct.unpack('<Q',memory(address,8))[0]
        def string(rva):length=struct.unpack('<I',at(rva,4))[0];return at(rva+4,length).decode('utf-16-le',errors='replace')
        modules=[]
        pos,_=streams[4];n=struct.unpack('<I',at(pos,4))[0]
        for index in range(n):
            data=at(pos+4+108*index,108)
            base,size,check,stamp,name=struct.unpack_from('<QIIII',data)
            modules.append({'base':base,'size':size,'path':string(name)})
        ex,_=streams[6];data=at(ex,168)
        thread_id=struct.unpack_from('<I',data)[0]
        code=struct.unpack_from('<I',data,8)[0]
        ctx_size,ctx_rva=struct.unpack_from('<II',data,160)
        context=at(ctx_rva,ctx_size)
        rip,rsp=struct.unpack_from('<Q',context,248)[0],struct.unpack_from('<Q',context,152)[0]
        pos,_=streams[3];n=struct.unpack('<I',at(pos,4))[0]
        command_mode=None;command_status='not captured'
        for index in range(n):
            data=at(pos+4+48*index,48)
            if struct.unpack_from('<I',data)[0]!=thread_id:continue
            teb=struct.unpack_from('<Q',data,16)[0]
            try:
                peb=pointer(teb+0x60);parameters=pointer(peb+0x20)
                length=struct.unpack('<H',memory(parameters+0x70,2))[0]
                buffer=pointer(parameters+0x78)
                if length>65534:raise ValueError('Invalid command length')
                command=memory(buffer,length).decode('utf-16-le',errors='replace')
                mode=re.search(r'--fork[a-zA-Z_]+',command)
                command_mode=mode[0] if mode else 'no fork mode found'
                command_status='read; only fork-mode token retained, all other arguments omitted'
            except (KeyError,ValueError):pass
        cache={}
        def describe(address):
            for module in modules:
                if module['base']<=address<module['base']+module['size']:
                    rva=address-module['base'];key=module['path']
                    if key not in cache:
                        try:cache[key]=pe_exports(key)
                        except (OSError,ValueError,struct.error):cache[key]=([],[])
                    exports,executable=cache[key]
                    if not any(start<=rva<end for start,end in executable):return None
                    item={'module':Path(key).name,'rva':hex(rva)}
                    i=bisect.bisect_right(exports,(rva,chr(0x10ffff)))-1
                    if i>=0:item['nearest_export']=exports[i][1];item['offset_from_export']=hex(rva-exports[i][0])
                    return item
            return None
        candidates=[]
        for n in range(256):
            try:address=pointer(rsp+8*n)
            except KeyError:break
            item=describe(address)
            if item:candidates.append({'stack_byte_offset':8*n,**item})
        report={'kind':'read_only_minidump_metadata_not_unwound_stack','dump_bytes':path.stat().st_size,'exception_code':hex(code),'exception_thread':thread_id,'instruction':describe(rip),'fork_mode':command_mode,'command_line_scope':command_status,'loaded_module_names':[Path(m['path']).name for m in modules],'candidate_stack_code_pointers':candidates,'privacy':'No raw memory, environment, SQL, data or complete command line retained; code pointers are candidates, not validated call frames.'}
        write_report(args.output,report)
        print({'output':str(args.output),'exception_code':report['exception_code'],'fork_mode':command_mode,'candidate_code_pointers':len(candidates),'scope':report['kind']})


if __name__=='__main__':main()
