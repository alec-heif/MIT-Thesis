writingFile = open('health_data_cleaned.csv', 'w')
with open('health_data.csv') as f:

    while True:

        c = f.read(1)
        if not c:
            print ("End of file")
            break
        print ("Read a character:", c)


        if c=='"':
            writingFile.write(c) 
            c = f.read(1)
            while c != '"':
                if c== ',':
                    c= '|'
                writingFile.write(c)
                c = f.read(1)


        writingFile.write(c)


writingFile.close()
