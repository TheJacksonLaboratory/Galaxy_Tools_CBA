
import sys
import optparse

def stop_err( msg ):
    sys.stderr.write( msg )
    sys.exit()

def main():
    usage = """%prog [options]

    """
    parser = optparse.OptionParser(usage=usage)

    #raise Exception('some informative error message')
    parser.add_option('-f','--file')
    parser.add_option('-s', '--string')


    options, args = parser.parse_args() 
    infile = open( options.file, 'r' )
    for line in infile:
        line = line.rstrip( '\r\n' )
        if line:
            print (line + " " + options.string)

if __name__ == "__main__": main()
