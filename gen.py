def compiled_schema_parser(src):
    obj = {'root': {}}
    obj["root"]["schemavsn"] = fastavro._read.read_utf8(src)
    obj["root"]["publisher"] = fastavro._read.read_utf8(src)
    obj["root"]["objectId"] = fastavro._read.read_utf8(src)
    obj["root"]["candid"] = fastavro._read.read_long(src)
    obj["root"]["candidate"] = {}
    obj["root"]["candidate"]["jd"] = fastavro._read.read_double(src)
    obj["root"]["candidate"]["fid"] = fastavro._read.read_long(src)
    obj["root"]["candidate"]["pid"] = fastavro._read.read_long(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["diffmaglim"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["pdiffimfilename"] = fastavro._read.read_utf8(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["programpi"] = fastavro._read.read_utf8(src)
    obj["root"]["candidate"]["programid"] = fastavro._read.read_long(src)
    obj["root"]["candidate"]["candid"] = fastavro._read.read_long(src)
    obj["root"]["candidate"]["isdiffpos"] = fastavro._read.read_utf8(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["tblid"] = fastavro._read.read_long(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["nid"] = fastavro._read.read_long(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["rcid"] = fastavro._read.read_long(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["field"] = fastavro._read.read_long(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["xpos"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["ypos"] = fastavro._read.read_float(src)
    obj["root"]["candidate"]["ra"] = fastavro._read.read_double(src)
    obj["root"]["candidate"]["dec"] = fastavro._read.read_double(src)
    obj["root"]["candidate"]["magpsf"] = fastavro._read.read_float(src)
    obj["root"]["candidate"]["sigmapsf"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["chipsf"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["magap"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["sigmagap"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["distnr"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["magnr"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["sigmagnr"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["chinr"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["sharpnr"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["sky"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["magdiff"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["fwhm"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["classtar"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["mindtoedge"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["magfromlim"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["seeratio"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["aimage"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["bimage"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["aimagerat"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["bimagerat"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["elong"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["nneg"] = fastavro._read.read_long(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["nbad"] = fastavro._read.read_long(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["rb"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["ssdistnr"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["ssmagnr"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["ssnamenr"] = fastavro._read.read_utf8(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["sumrat"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["magapbig"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["sigmagapbig"] = fastavro._read.read_float(src)
    obj["root"]["candidate"]["ranr"] = fastavro._read.read_double(src)
    obj["root"]["candidate"]["decnr"] = fastavro._read.read_double(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["sgmag1"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["srmag1"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["simag1"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["szmag1"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["sgscore1"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["distpsnr1"] = fastavro._read.read_float(src)
    obj["root"]["candidate"]["ndethist"] = fastavro._read.read_long(src)
    obj["root"]["candidate"]["ncovhist"] = fastavro._read.read_long(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["jdstarthist"] = fastavro._read.read_double(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["jdendhist"] = fastavro._read.read_double(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["scorr"] = fastavro._read.read_double(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["tooflag"] = fastavro._read.read_long(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["objectidps1"] = fastavro._read.read_long(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["objectidps2"] = fastavro._read.read_long(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["sgmag2"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["srmag2"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["simag2"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["szmag2"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["sgscore2"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["distpsnr2"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["objectidps3"] = fastavro._read.read_long(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["sgmag3"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["srmag3"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["simag3"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["szmag3"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["sgscore3"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["distpsnr3"] = fastavro._read.read_float(src)
    obj["root"]["candidate"]["nmtchps"] = fastavro._read.read_long(src)
    obj["root"]["candidate"]["rfid"] = fastavro._read.read_long(src)
    obj["root"]["candidate"]["jdstartref"] = fastavro._read.read_double(src)
    obj["root"]["candidate"]["jdendref"] = fastavro._read.read_double(src)
    obj["root"]["candidate"]["nframesref"] = fastavro._read.read_long(src)
    obj["root"]["candidate"]["rbversion"] = fastavro._read.read_utf8(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["dsnrms"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["ssnrms"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["dsdiff"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["magzpsci"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["magzpsciunc"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["magzpscirms"] = fastavro._read.read_float(src)
    obj["root"]["candidate"]["nmatches"] = fastavro._read.read_long(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["clrcoeff"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["clrcounc"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["zpclrcov"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["zpmed"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["clrmed"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["clrrms"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["neargaia"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["neargaiabright"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["maggaia"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["maggaiabright"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["exptime"] = fastavro._read.read_float(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["candidate"]["drb"] = fastavro._read.read_float(src)
    obj["root"]["candidate"]["drbversion"] = fastavro._read.read_utf8(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["prv_candidates"] = []
        blocksize = fastavro._read.read_long(src)
        while blocksize > 0:
            for _ in range(blocksize):
                obj["root"]["__tmpval"] = {}
                obj["root"]["__tmpval"]["jd"] = fastavro._read.read_double(src)
                obj["root"]["__tmpval"]["fid"] = fastavro._read.read_long(src)
                obj["root"]["__tmpval"]["pid"] = fastavro._read.read_long(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["diffmaglim"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["pdiffimfilename"] = fastavro._read.read_utf8(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["programpi"] = fastavro._read.read_utf8(src)
                obj["root"]["__tmpval"]["programid"] = fastavro._read.read_long(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["candid"] = fastavro._read.read_long(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["isdiffpos"] = fastavro._read.read_utf8(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["tblid"] = fastavro._read.read_long(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["nid"] = fastavro._read.read_long(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["rcid"] = fastavro._read.read_long(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["field"] = fastavro._read.read_long(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["xpos"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["ypos"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["ra"] = fastavro._read.read_double(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["dec"] = fastavro._read.read_double(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["magpsf"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["sigmapsf"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["chipsf"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["magap"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["sigmagap"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["distnr"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["magnr"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["sigmagnr"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["chinr"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["sharpnr"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["sky"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["magdiff"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["fwhm"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["classtar"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["mindtoedge"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["magfromlim"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["seeratio"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["aimage"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["bimage"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["aimagerat"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["bimagerat"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["elong"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["nneg"] = fastavro._read.read_long(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["nbad"] = fastavro._read.read_long(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["rb"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["ssdistnr"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["ssmagnr"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["ssnamenr"] = fastavro._read.read_utf8(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["sumrat"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["magapbig"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["sigmagapbig"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["ranr"] = fastavro._read.read_double(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["decnr"] = fastavro._read.read_double(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["scorr"] = fastavro._read.read_double(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["magzpsci"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["magzpsciunc"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["magzpscirms"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["clrcoeff"] = fastavro._read.read_float(src)
                idx = fastavro._read.read_long(src)
                if idx == 0:
                    pass
                elif idx == 1:
                    obj["root"]["__tmpval"]["clrcounc"] = fastavro._read.read_float(src)
                obj["root"]["__tmpval"]["rbversion"] = fastavro._read.read_utf8(src)
                obj["root"]["prv_candidates"].append(obj["root"]["__tmpval"])
                del obj["root"]["__tmpval"]
            blocksize = fastavro._read.read_long(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["cutoutScience"] = {}
        obj["root"]["cutoutScience"]["fileName"] = fastavro._read.read_utf8(src)
        obj["root"]["cutoutScience"]["stampData"] = fastavro._read.read_bytes(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["cutoutTemplate"] = {}
        obj["root"]["cutoutTemplate"]["fileName"] = fastavro._read.read_utf8(src)
        obj["root"]["cutoutTemplate"]["stampData"] = fastavro._read.read_bytes(src)
    idx = fastavro._read.read_long(src)
    if idx == 0:
        pass
    elif idx == 1:
        obj["root"]["cutoutDifference"] = {}
        obj["root"]["cutoutDifference"]["fileName"] = fastavro._read.read_utf8(src)
        obj["root"]["cutoutDifference"]["stampData"] = fastavro._read.read_bytes(src)
    return obj['root']
