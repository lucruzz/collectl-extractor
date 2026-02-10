#/bin/bash

INPUT_DIR="/home/lcruz/lncc/collectl-extractor/inputs/sdumont2nd/collectl/2026" # diretório de onde estão os traces
YEAR=$1 # "2023" # ano dos traces
MONTH_START=$2
MONTH_END=$3
SCHEMA=$4 # sdumont | sdumont2nd
# OUTPUT_DIR="./output_dir_list" # diretório de saída onde serão gerados uma lista com o nome dos arquivos dos traces
OUTPUT_DIR_RENAMED="/home/lcruz/lncc/collectl-extractor/outputs/sdumont2nd/collectl" # diretório de saída onde estarão os compactados já renomeados
OUTPUT_FILENAME_PATTERN="dirs_${YEAR}_" # arquivo de saída que contém a lista dos nomes dos arquivos dos traces

# Descrição: a ideia geral dessa função é criar um arquivo com a lista
# de todos os arquivos disponíveis no diretório do mês referente (de 1 a 12)
#
# Saída: são gerados arquivos no diretório de saída (OUTPUT_DIR)
# com os nomes dirs_ANO_MES.
list_directories() {
    local INPUT_DIR=$1
    local YEAR=$2
    local MONTH_START=$3
    local MONTH_END=$4
    # local OUTPUT_DIR="outputs_dir_list"
    # local OUTPUT_FILENAME_PATTERN="dirs_${YEAR}_"

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
        local output_file="${OUTPUT_DIR_RENAMED}/${YEAR}/${OUTPUT_FILENAME_PATTERN}${mes}"
        find ${tmp_dir} -maxdepth 1 -type f -printf "%f\n" > ${output_file}
    
    done
}

# Descrição: renomeia os arquivos dos traces utilizando a lita de arquivos 
# contida nos arquivos gerados em OUTPUT_DIR.
# 
# Saída: são gerados arquivos no diretório de saída OUTPUT_DIR_RENAMED
# prontos para serem descomprimidos.
renaming_files() {

    local MONTH_START=$1
    local MONTH_END=$2
    local SCHEMA=$3
    
    for num in $(seq $MONTH_START $MONTH_END); # seq 1 12
    do
        mes=""
        if [ $num -le 9 ]; then
            mes="0${num}"    
        else
            mes="${num}"
        fi
        infile=${OUTPUT_DIR_RENAMED}/${YEAR}/${OUTPUT_FILENAME_PATTERN}${mes}

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
                echo "$(date +"%d/%m/%Y %H:%M:%S") ERROR Not a valid schema not defined. It must be sdumont or sdumont2nd." 
            fi

            # configuro o caminho de destino
            dst="${OUTPUT_DIR_RENAMED}/${YEAR}/$mes/${clean2}"

            mkdir -p "${OUTPUT_DIR_RENAMED}/${YEAR}/${mes}"
            
            cp "$src" "$dst"
            
        done < $infile
    done
}

list_directories "$INPUT_DIR" "$YEAR" "$MONTH_START" "$MONTH_END"
renaming_files "$MONTH_START" "$MONTH_END" "$SCHEMA"