#!/usr/bin/env python

import optparse
import re
import sys

class WgetLogValidator:
    def __init__(self):
        # regular expressions:
        # regexp denoting successful collection index/directory listing save:
        self.cindex      = re.compile(r'(index|listing).*saved')

        # regexp for timestamp string containing an 'http' hdf (or xml) target:
        self.timestamp   = r'--\d{4}-\d{2}-\d{2} *\d{2}:\d{2}:\d{2}--'
        self.target      = r'(http|ftp).*\.(hdf|nc).*$'
        self.timestamp_and_target = re.compile( self.timestamp + ' *' + '(' + self.target + ')')    # save target as group

        # regexp denoting target was successfully downloaded (saved):
        self.saved       = re.compile(r'saved')

        # regexp denoting overall completion:
        self.finished    = re.compile(r'FINISHED')

    def validate_logs(self, *logfiles):
        for logfilename in logfiles:

            # logicals, re match objects for checking status of download requests:

            cindex_present  = False         # collection index presence
            downloading     = False         # True at start of a reqest, False when complete.
            err             = False         # True for incomplete downloads.
            tt_match        = None          # timestamp and target match object.
            tt_match_prev   = None          # prior match object for possible failed request
                                            # reporting.
            tt_match_this_line  = False     # timestamp and target match on this line.
            _finished_      = False         # 'FINISHED' all files in collection
            saved_match     = []            # 'saved' string match object
            errlist         = []            # list of failed download targets.

            logfile = ''
            try:
                logfile = open(logfilename)
            except IOError as e:
                print e;

            # for individual file download checks, scan through the "noise" in the file,
            # looking for typical download start/end signatures, taking note of those that
            # are incomplete:

            for line in logfile:

                # timestamp and target request match?
                if self.timestamp_and_target.match(line):

                #if timestamp_and_target.match(line) and not get_request.search(line):
                    tt_match_this_line=True
                    if tt_match:
                        tt_match_prev = tt_match
                    tt_match = self.timestamp_and_target.match(line)

                # "saved" string match?
                saved_match = self.saved.search(line)

                # check process status:
                if tt_match_this_line and not downloading:
                    # start of new request
                    downloading = True
                elif tt_match_this_line and downloading and \
                     tt_match.group(1)!=tt_match_prev.group(1):
                    # new download request without successful completion of prior one
                    # (sometimes wget attempts to re-download a target, which is perfectly
                    # ok, thus the check against the previous target)
                    err = True
                elif downloading and saved_match:
                    # successful completion; reset logicals
                    downloading = False
                    saved_match = None
                    print tt_match.group(1)
                elif self.finished.match(line):
                    _finished_  = True

                if err:
                    # capture target of failed request
                    errlist.append(tt_match_prev.group(1))

                # continue download error search:
                err = False
                tt_match_this_line = False

            # last attempted download complete?:
            if not _finished_:
                try:
                    errlist.append('download aborted during read of '+tt_match.group(1))
                except:
                    errlist.append('download aborted.')

            # summary output:
            if errlist.__len__():
                print '%s:' % logfilename
                print '   error(s) downloading the following files in the collection:'
                for failed_target in errlist:
                    print '   %s' % failed_target

if __name__ == '__main__':
    w = WgetLogValidator()
    w.validate_logs('/Users/asmuniz/Desktop/develop/project-info/gregg_data_processing/success.log')