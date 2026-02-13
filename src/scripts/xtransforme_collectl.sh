#/bin/bash

YEAR=$1 # "2023" # ano dos traces
MONTH_START=$2
MONTH_END=$3
SCHEMA=$4 # sdumont | sdumont2nd
# OUTPUT_DIR="./output_dir_list" # diretório de saída onde serão gerados uma lista com o nome dos arquivos dos traces
# INPUT_DIR="/home/lcruz/lncc/collectl-extractor/inputs/sdumont2nd/collectl/2026" # diretório de onde estão os traces
# OUTPUT_DIR_RENAMED="/home/lcruz/lncc/collectl-extractor/outputs/sdumont2nd/collectl" # diretório de saída onde estarão os compactados já renomeados
INPUT_DIR=$5
OUTPUT_DIR_RENAMED=$6

OUTPUT_FILENAME_PATTERN_TAR="dirs_${YEAR}_" # arquivo de saída que contém a lista dos nomes dos arquivos dos traces
OUTPUT_FILENAME_PATTERN_NODELIST="io_${YEAR}_"

# Descrição: a ideia geral dessa função é criar um arquivo com a lista
# de todos os arquivos disponíveis no diretório do mês referente (de 1 a 12)
#
# Saída: são gerados arquivos no diretório de saída (OUTPUT_DIR)
# com os nomes dirs_ANO_MES.
list_directories() {

    local YEAR=$1
    local MONTH_START=$2
    local MONTH_END=$3
    local SCHEMA=$4
    local INPUT_DIR=$5
    local OUTPUT_DIR_RENAMED=$6
    # local OUTPUT_DIR="outputs_dir_list"
    # local OUTPUT_FILENAME_PATTERN_TAR="dirs_${YEAR}_"

    mkdir -p ${OUTPUT_DIR_RENAMED}/${YEAR}

    for num in $(seq $MONTH_START $MONTH_END); # seq 1 12
    do
        
        local mes=""
        if [ $num -le 9 ]; then
            mes="0${num}"
            echo "$(date +"%d/%m/%Y %H:%M:%S") INFO Reading files from directory: $INPUT_DIR/$mes"
            
        else
            mes="${num}"
            echo "$(date +"%d/%m/%Y %H:%M:%S") INFO Reading files from directory: $INPUT_DIR/$mes"    
        fi

        local tmp_dir="${INPUT_DIR}/${mes}"
        local output_file_tarname="${OUTPUT_DIR_RENAMED}/${YEAR}/${OUTPUT_FILENAME_PATTERN_TAR}${mes}"
        find ${tmp_dir} -maxdepth 1 -type f -printf "%f\n" > ${output_file_tarname}

        local output_file_io_nodename="${OUTPUT_DIR_RENAMED}/${YEAR}/${OUTPUT_FILENAME_PATTERN_NODELIST}${mes}.csv"
        
        if [ "$SCHEMA" = "sdumont" ]; then
            awk -F. '{print $NF}' ${output_file_tarname} | sort | uniq > ${output_file_io_nodename}
        elif [ "$SCHEMA" = "sdumont2nd" ]; then
            # awk -F- '{print $2}' ${output_file_tarname} | sort | uniq > ${output_file_io_nodename}
            awk -F- '{print $1","$2","$3}' ${output_file_tarname} | awk -F. '{print $1","$2}' | awk -FLLITE_LLDET '{print $1}' > ${output_file_io_nodename}
        else
            echo "$(date +"%d/%m/%Y %H:%M:%S") ERROR Not a valid schema or schema not defined. It must be sdumont or sdumont2nd."
        fi
    
    done
}

# Descrição: renomeia os arquivos dos traces utilizando a lita de arquivos 
# contida nos arquivos gerados em OUTPUT_DIR.
# 
# Saída: são gerados arquivos no diretório de saída OUTPUT_DIR_RENAMED
# prontos para serem descomprimidos.
renaming_files() {

    local YEAR=$1
    local MONTH_START=$2
    local MONTH_END=$3
    local SCHEMA=$4
    local INPUT_DIR=$5
    local OUTPUT_DIR_RENAMED=$6
    
    for num in $(seq $MONTH_START $MONTH_END); # seq 1 12
    do
        mes=""
        if [ $num -le 9 ]; then
            mes="0${num}"    
        else
            mes="${num}"
        fi
        infile=${OUTPUT_DIR_RENAMED}/${YEAR}/${OUTPUT_FILENAME_PATTERN_TAR}${mes}

        echo "$(date +"%d/%m/%Y %H:%M:%S") INFO Renaming files listed in ${infile} to be unzipped."

        while IFS= read -r line; do
            # path do arquivo original
            src="${INPUT_DIR}/${mes}/${line}"

            if [ "$SCHEMA" = "sdumont" ]; then
                # limpo o nome, tirando o "LLITE|LLDET"
                clean1=$(echo "$line" | sed 's/\.LLITE|LLDET//g')

                # limpo o nome tirando o ".sdumont*" no final 
                clean2=$(echo "$clean1" | sed 's/\.sdumont[0-9]\+$//')
            
                if echo "$clean2" | grep -Pq '^(mdt|ost)-sdumont\d{1,4}-\d{8}\.(tab\.gz|gz)$'; then
                    echo "$(date +"%d/%m/%Y %H:%M:%S") INFO File ${clean2} is valid." 
                else
                    echo "$(date +"%d/%m/%Y %H:%M:%S") INFO File ${clean2} is invalid it does not follow a pattern." 
                fi
            elif [ "$SCHEMA" = "sdumont2nd" ]; then
                # limpo o nome, tirando o "LLITE_LLDET"
                clean1=$(echo "$line" | sed -E 's/\.LLITE_LLDET//')

                # limpo o nome tirando o ".sdumont2nd*" no final 
                clean2=$(echo "$clean1" | sed 's/\.sdumont2nd[0-9]\+$//')
            
                if echo "$clean2" | grep -Pq '^(mdt|ost)-sdumont2nd\d{1,4}-\d{8}\.(tab\.gz|gz)$'; then
                    echo "$(date +"%d/%m/%Y %H:%M:%S") INFO File ${clean2} is valid." 
                else
                    echo "$(date +"%d/%m/%Y %H:%M:%S") INFO File ${clean2} is invalid it does not follow a pattern." 
                fi
            else
                echo "$(date +"%d/%m/%Y %H:%M:%S") ERROR Not a valid schema or schema not defined. It must be sdumont or sdumont2nd." 
            fi

            # configuro o caminho de destino
            dst="${OUTPUT_DIR_RENAMED}/${YEAR}/$mes/${clean2}"

            mkdir -p "${OUTPUT_DIR_RENAMED}/${YEAR}/${mes}"
            
            cp "$src" "$dst"
            
        done < $infile
    done
}

list_directories "$YEAR" "$MONTH_START" "$MONTH_END" "$SCHEMA" "$INPUT_DIR" "$OUTPUT_DIR_RENAMED"
renaming_files "$YEAR" "$MONTH_START" "$MONTH_END" "$SCHEMA" "$INPUT_DIR" "$OUTPUT_DIR_RENAMED"