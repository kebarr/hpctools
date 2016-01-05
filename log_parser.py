class LSFLog():
    def __init__(self, log_lines):
        self.log_lines = log_lines
        self.options = {}
        self.errors = [0,0,0]# realistically if the first fails, all will
        self.commands_run = []
        self.parse_log()

    def parse_log(self):
        try:
            self.parse_first_section()
        except Exception as e:
            self.errors[0] = e 
        try:
            self.parse_second_section()
        except Exception as e:
            self.errors[1] = e
        try:
            self.parse_third_section()
        except Exception as e:
            self.errors[2] = e

    def parse_first_section(self):
        self.first_section_first_line_index = 0
        self.name = self.log_lines[2].split('<')[1].split('>')[0]
        self.home_directory = self.log_lines[4].split('<')[1].split('>')[0]
        self.working_directory = self.log_lines[5].split('<')[1].split('>')[0]
        self.start_time_string = self.log_lines[6].split('at ')[1]
        self.results_time_string = self.log_lines[7].split('at ')[1]
        for i, line in enumerate(self.log_lines):
            line_words = line.split(' ')
            # if we're at the bottom, its a string of '=='
            if len(line_words) == 1:
                if set(list(line_words[0].strip())) == set(['-']):
                    self.second_section_first_line_index = i + 1
                    break


    def parse_second_section(self):
        # second section is submitted script, get memory requested etc
        ind = self.second_section_first_line_index
        second_section = self.log_lines[ind:]
        for i, line in enumerate(second_section):
            line_words = line.split(' ')
            # if we're at the bottom, its a string of '--'
            if len(line_words) == 1:
                if set(list(line_words[0].strip())) == set(['-']):
                    self.third_section_first_line_index = ind + i
                    break
            if line_words[0].strip() == '#BSUB':
                self.get_bsub_option(line_words)
            else:
                self.commands_run.append(line)


    def parse_third_section(self):
        summary_section = self.log_lines[self.third_section_first_line_index + 1:]
        result = summary_section[0]
        self.success = result.startswith('S')
        self.exit_reason = None
        if not self.success:
            try:
                self.exit_reason = int(result.split(' ')[-1].split('.')[0])
                self.assign_usage_values(summary_section)
            except ValueError:
                self.exit_reason = result
                self.assign_usage_values(summary_section)

    def assign_usage_values(self, summary_section):
        self.cpu_time = self.get_resource_usage(summary_section[self.get_line_index('CPU time', summary_section)], 'CPU time')
        self.total_requested_memory = self.get_resource_usage(summary_section[self.get_line_index('Total Requested Memory', summary_section)], 'Total Requested Memory')
        delta_memory_line = summary_section[self.get_line_index('Delta Memory',summary_section)]
        if delta_memory_line.split('Delta Memory :')[1].strip().split(' ')[0] != '-':
            self.delta_memory = self.get_resource_usage( delta_memory_line, 'Delta Memory')
        else:
            self.delta_memory = None
        memory_line_index = self.get_line_index('Max Memory', summary_section)
        if memory_line_index:
            max_memory_line = summary_section[memory_line_index]
            self.max_memory = self.get_resource_usage(max_memory_line, 'Max Memory')
        avg_line_index = self.get_line_index('Average Memory', summary_section)
        if avg_line_index:
            avg_memory_line = summary_section[avg_line_index]
            self.avg_memory = self.get_resource_usage(avg_memory_line, 'Average Memory')
        max_swap_index = self.get_line_index('Max Swap', summary_section)
        if max_swap_index:
            max_swap_line = summary_section[max_swap_index]
            self.max_swap = self.get_resource_usage(max_swap_line, 'Max Swap')
        max_processes_index = self.get_line_index('Max Processes', summary_section)
        if max_processes_index:
            max_processes_line = summary_section[max_processes_index]
            self.max_processes = self.get_resource_usage(max_processes_line, 'Max Processes')
        max_threads_index = self.get_line_index('Max Threads', summary_section)
        if max_threads_index:
            max_threads_line = summary_section[max_threads_index]
            self.max_threads = self.get_resource_usage(max_threads_line, 'Max Threads')

    def get_line_index(self, attribute, list_of_strings):
        for string in list_of_strings:
            if attribute in string:
                return list_of_strings.index(string)
            

    def get_resource_usage(self, usage_line, resource_name):
        return float(usage_line.split(resource_name+' :')[1].strip().split(' ')[0])

    def get_bsub_option(self, bsub_line):
        print(bsub_line)
        option = bsub_line[1]
        argument = bsub_line[2:]
        bsub_options = {'-q': 'queue', '-n':'cores', '-J':'job name', '-R':'memory usage', '-W':'requested time', '-e':'errorfile name', 'o':'outfile name'}
        try:
            option_name = bsub_options[option]
        except KeyError:
            raise NotImplementedError("Bsub option %s not recognised" % (option))
        if option == 'memory usage':
            self.options[option_name] = int(argument[1].split('=')[1].split(']')[0])
        elif option == 'requested time':
            self.options[option_name] = int(argument[0])
        else:
            self.options[option_name] = argument[0].strip()            
        


class LSFLogFile():
    def __init__(self, log_file_name, command_names=['bwa aln', 'sickle']):
        self.filename = log_file_name
        self.logs = []
        self.separate_logs()
        self.command_names = command_names
        self.job_type = self.log_type()


    def separate_logs(self):
        log_lines = []
        with open(self.filename, 'r') as f:
            for i, line in enumerate(f.readlines()):
                line = line if line != '\n' else ''
                if line:
                    first_word_of_line = line.split(' ')[0]
                    # logs for same shell script are appended, so separate them
                    if first_word_of_line == 'Sender:' and i != 0:
                        log = LSFLog(log_lines)
                        self.logs.append(log)
                        log_lines = [line]
                    else:
                        log_lines.append(line)
            log = LSFLog(log_lines)
            self.logs.append(log)

    # assumes single job is run, otherwise type is first match found
    def log_type(self):
        for log in self.logs:
            for command in log.commands_run:
                for name in self.command_names:
                    if name in command:
                        return name
        return 'unknown'

