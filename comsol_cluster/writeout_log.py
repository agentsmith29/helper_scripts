class ClusterLogParser():

    def __init__(self, file_name):
        self._file_name = file_name

    def _follow_file(file, sleep_sec=0.1) -> Iterator[str]:
        """ Yield each line from a file as they are written.
        `sleep_sec` is the time to sleep after empty reads. """
        line = ''
        while True:
            tmp = file.readline()
            if tmp is not None and tmp != "":
                line += tmp
                if line.endswith("\n"):
                    yield line
                    line = ''
            elif sleep_sec:
                time.sleep(sleep_sec)

if __name__ == '__main__':
    with open("teslog.log", 'r') as file:
        for line in follow(file):
            print(line, end='')