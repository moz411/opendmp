import xdrlib

#
class Packer(xdrlib.Packer):

    def pack_fstring(self, n, s):
        if n < 0:
            raise ValueError('fstring size must be nonnegative')
        data = s
        r = n
        n = ((n+3)//4)*4
        data = (data + (n - r) * b'\0')
        self.__buf.write(data)


class Unpacker(xdrlib.Unpacker):

    def unpack_fstring(self, n):
        if n < 0:
            raise ValueError('fstring size must be nonnegative')
        i = self.__pos
        j = i + (n+3)//4*4
        if j > len(self.__buf):
            raise EOFError
        self.__pos = j
        return self.__buf[i:i+n]
