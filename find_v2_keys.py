import os
import re
import struct
import glob
import time
import ctypes
from ctypes import wintypes
from Crypto.Cipher import AES
from Crypto.Util import Padding

PROCESS_ALL_ACCESS = 0x1F0FFF
PROCESS_VM_READ = 0x0010
PROCESS_QUERY_INFORMATION = 0x0400
MEM_COMMIT = 0x1000
PAGE_NOACCESS = 0x01
PAGE_GUARD = 0x100
PAGE_READWRITE = 0x04
PAGE_WRITECOPY = 0x08
PAGE_EXECUTE_READWRITE = 0x40
PAGE_EXECUTE_WRITECOPY = 0x80

class MEMORY_BASIC_INFORMATION(ctypes.Structure):
    _fields_ = [
        ("BaseAddress", ctypes.c_void_p),
        ("AllocationBase", ctypes.c_void_p),
        ("AllocationProtect", wintypes.DWORD),
        ("RegionSize", ctypes.c_size_t),
        ("State", wintypes.DWORD),
        ("Protect", wintypes.DWORD),
        ("Type", wintypes.DWORD),
    ]

kernel32 = ctypes.windll.kernel32
RE_KEY32 = re.compile(rb'(?<![a-zA-Z0-9])[a-zA-Z0-9]{32}(?![a-zA-Z0-9])')
RE_KEY16 = re.compile(rb'(?<![a-zA-Z0-9])[a-zA-Z0-9]{16}(?![a-zA-Z0-9])')

def get_wechat_pids():
    import subprocess
    result = subprocess.run(
        ['tasklist.exe', '/FI', 'IMAGENAME eq Weixin.exe', '/FO', 'CSV', '/NH'],
        capture_output=True, text=True
    )
    pids = []
    for line in result.stdout.strip().split('\n'):
        if 'Weixin.exe' in line:
            parts = line.strip('"').split('","')
            if len(parts) >= 2:
                pids.append(int(parts[1]))
    return pids

def find_v2_ciphertext(attach_dir):
    v2_magic = b'\x07\x08V2\x08\x07'
    pattern = os.path.join(attach_dir, "*", "*", "*", "*_t.dat")
    dat_files = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
    if not dat_files:
        pattern = os.path.join(attach_dir, "*", "*", "Img", "*_t.dat")
        dat_files = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)

    for f in dat_files[:100]:
        try:
            with open(f, 'rb') as fp:
                header = fp.read(31)
            if header[:6] == v2_magic and len(header) >= 31:
                return header[15:31], os.path.basename(f)
        except:
            pass
    return None, None

def try_key(key_bytes, ciphertext):
    try:
        cipher = AES.new(key_bytes, AES.MODE_ECB)
        dec = cipher.decrypt(ciphertext)
        if dec[:3] == b'\xFF\xD8\xFF': return 'JPEG'
        if dec[:4] == b'\x89PNG': return 'PNG'
        if dec[:4] == b'RIFF': return 'WEBP'
        if dec[:4] == b'wxgf': return 'WXGF'
        if dec[:3] == b'GIF': return 'GIF'
    except:
        pass
    return None

def scan_memory_for_aes_key(pid, ciphertext):
    access = PROCESS_VM_READ | PROCESS_QUERY_INFORMATION
    h_process = kernel32.OpenProcess(access, False, pid)
    if not h_process: return None
    try:
        address = 0
        mbi = MEMORY_BASIC_INFORMATION()
        regions = []
        while address < 0x7FFFFFFFFFFF:
            if kernel32.VirtualQueryEx(h_process, ctypes.c_void_p(address), ctypes.byref(mbi), ctypes.sizeof(mbi)) == 0:
                break
            if mbi.State == MEM_COMMIT and mbi.Protect != PAGE_NOACCESS and (mbi.Protect & PAGE_GUARD) == 0:
                regions.append((mbi.BaseAddress, mbi.RegionSize))
            next_addr = address + mbi.RegionSize
            if next_addr <= address: break
            address = next_addr

        for base_addr, region_size in regions:
            buffer = ctypes.create_string_buffer(region_size)
            bytes_read = ctypes.c_size_t(0)
            if not kernel32.ReadProcessMemory(h_process, ctypes.c_void_p(base_addr), buffer, region_size, ctypes.byref(bytes_read)): continue
            if bytes_read.value < 32: continue
            data = buffer.raw[:bytes_read.value]

            for m in RE_KEY32.finditer(data):
                k = m.group()
                if try_key(k[:16], ciphertext): return k[:16].decode('ascii')
                if try_key(k, ciphertext): return k.decode('ascii')
            for m in RE_KEY16.finditer(data):
                k = m.group()
                if try_key(k, ciphertext): return k.decode('ascii')
        return None
    finally:
        kernel32.CloseHandle(h_process)

def find_xor_key(attach_dir):
    v2_magic = b'\x07\x08V2\x08\x07'
    pattern = os.path.join(attach_dir, "*", "*", "*", "*_t.dat")
    dat_files = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
    if not dat_files:
        pattern = os.path.join(attach_dir, "*", "*", "Img", "*_t.dat")
        dat_files = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)

    counts = {}
    for f in dat_files[:32]:
        try:
            with open(f, 'rb') as fp:
                head = fp.read(6)
                if head != v2_magic: continue
                fp.seek(os.path.getsize(f) - 2)
                tail = fp.read(2)
                if len(tail) == 2:
                    k = (tail[0], tail[1])
                    counts[k] = counts.get(k, 0) + 1
        except: pass
    if not counts: return 0x88
    x, y = max(counts, key=counts.get)
    return x ^ 0xFF

if __name__ == '__main__':
    print("Buscando pids do WeChat...")
    pids = get_wechat_pids()
    if not pids: print("WeChat fechado.")
    else:
        attach_dir = r"C:\Users\Felipe\Documents\xwechat_files\wxid_xd3703k0ih2p22_f402\msg\attach"
        import json
        ct, fname = find_v2_ciphertext(attach_dir)
        if ct:
            for pid in pids:
                print(f"Buscando chave AES na mem do PID {pid}...")
                key = scan_memory_for_aes_key(pid, ct)
                if key:
                    xor = find_xor_key(attach_dir)
                    print(f"SUCESSO! AES={key} XOR=0x{xor:02x}")
                    with open("v2_keys.json", "w") as f:
                        json.dump({"image_aes_key": key, "image_xor_key": xor}, f)
                    import sys; sys.exit(0)
            print("Chave nao encontrada. Abra uma imagem no WeChat e tente de novo.")
        else:
            print("Nenhuma imagem _t.dat V2 encontrada.")
