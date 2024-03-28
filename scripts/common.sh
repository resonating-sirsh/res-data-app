#!/usr/bin/env bash

which yq > /dev/null 2>&1 || die "Cannot find yq!"

declare -A col=(
[Reset]='\033[0m'
# Regular             Bold                   Underline              
#      High Intensity        BoldHigh Intens     Background         
#           High Intensity Backgrounds
[Blk]='\033[0;30m'    [BBlk]='\033[1;30m'    [UBlk]='\033[4;30m'
      [IBlk]='\033[0;90m'    [BIBlk]='\033[1;90m'   [On_Blk]='\033[40m'
            [On_IBlk]='\033[0;100m'
[Red]='\033[0;31m'    [BRed]='\033[1;31m'    [URed]='\033[4;31m'
      [IRed]='\033[0;91m'    [BIRed]='\033[1;91m'   [On_Red]='\033[41m'
            [On_IRed]='\033[0;101m'
[Grn]='\033[0;32m'    [BGrn]='\033[1;32m'    [UGrn]='\033[4;32m'
      [IGrn]='\033[0;92m'    [BIGrn]='\033[1;92m'   [On_Grn]='\033[42m'
            [On_IGrn]='\033[0;102m'
[Yel]='\033[0;33m'    [BYel]='\033[1;33m'    [UYel]='\033[4;33m'
      [IYel]='\033[0;93m'    [BIYel]='\033[1;93m'   [On_Yel]='\033[43m'
            [On_IYel]='\033[0;103m'
[Blu]='\033[0;34m'    [BBlu]='\033[1;34m'    [UBlu]='\033[4;34m'
      [IBlu]='\033[0;94m'    [BIBlu]='\033[1;94m'   [On_Blu]='\033[44m'
            [On_IBlu]='\033[0;104m'
[Pur]='\033[0;35m'    [BPur]='\033[1;35m'    [UPur]='\033[4;35m'
      [IPur]='\033[0;95m'    [BIPur]='\033[1;95m'   [On_Pur]='\033[45m'
            [On_IPur]='\033[0;105m'
[Cya]='\033[0;36m'    [BCya]='\033[1;36m'    [UCya]='\033[4;36m'
      [ICya]='\033[0;96m'    [BICya]='\033[1;96m'   [On_Cya]='\033[46m'
            [On_ICya]='\033[0;106m'
[Whi]='\033[0;37m'    [BWhi]='\033[1;37m'    [UWhi]='\033[4;37m'
      [IWhi]='\033[0;97m'    [BIWhi]='\033[1;97m'   [On_Whi]='\033[47m'
            [On_IWhi]='\033[0;107m'
)
fail_with() {
    fail="return"
    if [[ $2 == "exit" ]]; then
        fail="exit"
    fi
    if [[ $1 -gt 0 ]]; then
        "$fail" "$1";
    else
        "$fail" 1
    fi
}
die() {
    err=$?
    printf \
        "${col[On_IRed]}${col[BIWhi]}%s${col[Reset]}  ${col[On_Blk]}${col[IRed]}%s${col[Reset]}\n" \
        "ERR!" "$*" >&2
    fail_with $err "exit"
}
warn () {
    printf \
        "${col[On_IYel]}${col[BBlk]}%s${col[Reset]}  ${col[On_Blk]}${col[IYel]}%s${col[Reset]}" \
        "WARN!" "$*" >&2
}
warnn() { warn "$@"; echo >&2; }
info () {
    printf \
        "${col[On_Blu]}${col[BIWhi]}%s${col[Reset]}  ${col[On_Blk]}${col[IBlu]}%s${col[Reset]}" \
        " :>> " "$*" >&2
}
infon() { info "$@" >&2; echo >&2; }
debug () {
    if [[ -n $DEBUG ]]; then
        printf \
            "${col[On_ICya]}${col[BIBlk]}%s${col[Reset]}  ${col[On_IBlk]}${col[BCya]}%s${col[Reset]}" \
            " +|> " "$*" >&2
    fi
}
debugn() { debug "$@" >&2; echo >&2; }
color() { printf "${col[$1]}%s${col[$3]}" "$2"; }

init_cleanup() {
    trap -p SIGINT SIGQUIT SIGTERM EXIT | grep -c '_cleanup_'
    if 
        (( $(trap -p SIGINT SIGQUIT SIGTERM EXIT | grep -c '_cleanup_') >= 3 ))
        [[ -f $_cleanup_tasks_ ]]
    then
        debugn "Cleanup already initialized!"
        return 0
    fi
    debugn "Initialing cleanup..."

    _cleanup_tasks_="$(mktemp -t cleanup_tasks_XXXXXX)"
    export _cleanup_tasks_
    debugn "Cleanup tasks list: $_cleanup_tasks_"
    _cleanup_() {
        local err=$?
        trap '' SIGINT SIGQUIT SIGTERM EXIT
        # shellcheck source=/dev/null
        local _cleanup_tasks_log_="${_cleanup_tasks_}.log" task_ctr=0
        cat <<EOF >>"$_cleanup_tasks_log_"
######### CLEANUP LOG: PID $$ #########
Cleanup tasks list: ${_cleanup_tasks_}
EOF

        while read -r taskfile; do
            (( task_ctr++ ))
            cat <<EOF >>"$_cleanup_tasks_log_"
########## TASK #${task_ctr} ##########
Cleanup taskfile: ${taskfile}
#### ${taskfile} USERCODE FOLLOWS:
EOF
            sed -e 's/^/## /' "$taskfile" >>"$_cleanup_tasks_log_"
            # shellcheck source=/dev/null

            if . "$taskfile" >> "$_cleanup_tasks_log_"; then
                rm -f "$taskfile"
            else
                message="Cleanup task in $taskfile failed with $?!"
                debugn "$message"
                cat <<EOF >>"$_cleanup_tasks_log_"

#####
### $message
#####

EOF
            fi
        done < "$_cleanup_tasks_"
        if [[ -z $DEBUG ]]; then
            rm -f "$_cleanup_tasks_"
        fi
        debugn "View cleanup tasks log here: $_cleanup_tasks_log_"
        exit $err
    }
    _sig_cleanup_() {
        local err=$?
        trap '' EXIT
        set +e
        fail_with $err
        _cleanup_
    }
    trap "_cleanup_" EXIT
    trap "_sig_cleanup_" SIGINT SIGQUIT SIGTERM
    export -f fail_with _cleanup_ _sig_cleanup_
}

add_cleanup_task() {
    init_cleanup
    debugn "Cleanup tasks list: $_cleanup_tasks_"
    local _cleanup_task_ctr_ _cleanup_taskfile_ title
    _cleanup_task_ctr_="$(wc -l < "$_cleanup_tasks_")"
    _cleanup_taskfile_="$(mktemp -t "cleanup_task_${_cleanup_task_ctr_:-0}_XXXXXX")"
    title="cleanup task #$_cleanup_task_ctr_.${1:+ $1}"
    cat <(cat <<EOF
# ${title@u}
echo "Running $title..."

EOF
    ) - <(cat <<EOF

echo "${title@u} done!"
EOF
) >> "$_cleanup_taskfile_"
    debugn "Added ${title} ($_cleanup_taskfile_)"
}

parse_params_opts() {
    local old_extglob
    old_extglob="$(shopt -p extglob)"
    shopt -s extglob
    declare -p params >/dev/null || declare -a params=()
    declare -p opts >/dev/null || declare -A opts=()
    g_or() { printf "%s|" "${@:1:$#-1}"; printf "%s" "${@: -1}"; }
    g_any() { printf "@(%s)" "$(g_or "$@")"; }
    g_brk() { printf "\[%s\]" "$(g_any "$@")";  }
    nonopt="!($(g_or "${!opts[@]}" "$(g_brk "${!opts[@]}")" "$(g_any "${!opts[@]}" "$(g_brk "${!opts[@]}")")")=*|--)"
    while (( $# > 0 )); do
        # shellcheck disable=SC2254
        case "$1" in
            $(g_brk "${!opts[@]}")=* ) set -- "${1%%=*}" "${1##*=}" "${@:2}" ;;
            $(g_brk "${!opts[@]}")   ) set -- "${1//[\[\]]/}" "${@:2}"       ;;
            $(g_any "${!opts[@]}")   ) set -- "$1=$2" "${@:3}"               ;;
            $nonopt                  ) params+=( "$1" )                      ;;&
            $(g_any "${!opts[@]}")=* ) opts+=( ["${1%%=*}"]="${1##*=}")      ;;&
            --                       ) set -- "${@// /_}"; nonopt="!(*)"     ;;&
            *                        ) shift                                 ;;
        esac
    done
    # shellcheck source=/dev/null
    . <(echo "$old_extglob")
}

export clear_forward="\033[0K"
export clear_back="\033[1K"
export clear_line="\033[2K"
export clear_down="\033[0J"
export clear_up="\033[1J"

move_cursor() {
    local dir
    case "${1,,*}" in
        beg|beginning) printf "\r"; return 0 ;;
        end) printf "\033[%dC" "${COLUMNS:-200}"; return 0 ;;
        up) dir="A" ;;
        dw|down) dir="B" ;;
        rt|fwd|right|forward) dir="C" ;;
        lt|bck|left|back) dir="D" ;;
        a|b|c|d|e|f|g|h) dir="$1" ;;
        to|pos|position) [[ $# -eq 3 ]] && dir="H" || dir="G" ;;
        [0-9]*) die "First argument to move_cursor must be a direction! (got: ${*@Q})" ;;
        *) die "Invalid direction '$1'! got: ${*@Q})" ;;
    esac
    printf "\033[%d;%d%c" "$2" "$3" "$dir"
}

declare -A connection_status=( 
    [down]="${col[BRed]}✘${col[Reset]}"
    [up]="${col[BGrn]}✔${col[Reset]}"
)

wait_for_up() {
    local -a urls=( "$@" )
    local -a devnulls
    read -ra devnulls < <(printf -- "-o/dev/null %0.s" "${urls[@]}")
    curldone="$(mktemp)"
    results="$(mktemp)"
    # printf "%s ${connection_status[down]}\n" "${urls[@]}" >"$results"
    rm -rf "$curldone"

    coproc {
        curl -Z -s -m1 --connect-timeout 0.5 \
            --retry 20 --retry-delay 2 --retry-all-errors \
            -w "%{url}\n" \
            "${urls[@]}" "${devnulls[@]}" \
            >> "$results"
        printf "DONE!" > "$curldone"
    }

    
    # shellcheck disable=SC2034
    local cycle_time=0.1 cycles=0 update=s nrows="$(( ${#urls[@]} + 1 ))"
    local waiting_msg="Waiting for services to be up" time_width=5
    local counter_len="$(( (COLUMNS ? COLUMNS : 79) - ${#waiting_msg} - time_width - 4 ))" # == 41
    update_print_area() {
        conn_results="$(sort -u <"$results")"
        read -ra connected_urls \
            < <(xargs printf "%s ${connection_status[up]}\n" <<<"$conn_results" | xargs)
        read -ra disconnected_urls < <(
            comm -23 <(printf "%s\n" "${urls[@]}" | sort) <(echo "$conn_results") \
                | xargs printf "%s ${connection_status[down]}\n" | xargs
        )
        local -a updates=()
        case "$update" in
            c)
                updates+=( "$(move_cursor beginning)" "$(move_cursor up $nrows)" "$clear_down" )
                ;&
            s)  updates+=(
                    "Requested services:\n"
                    "$(printf " - %s %b\n" "${disconnected_urls[@]}" "${connected_urls[@]}" | sort)"
                    "$(
                        printf "\n%s |%-${counter_len}s|%${time_width}.1fs" \
                        "$waiting_msg" \
                        "$(printf "%0$(((cycles % counter_len)))d." "" | tr '0' '.')" \
                        "$(bc <<< "$cycles * $cycle_time")"
                    )"
                )
                ;;
        esac
        printf "%b" "${updates[@]}"
        update=c
    }
    while : ; do
        update_print_area
        if [[ -f $curldone ]]; then
            rm -rf "$curldone"
            break
        fi
        (( cycles++ ))
        sleep "$cycle_time"
    done
    echo
    grep -qsv "000" "$results"
    rm -rf "$results"
    retcode="$?"
    return "$retcode"
}

inquire_user_yes_NO() {
    local -a params=()
    local -A opts=( [default]=no [force]=no [level]=info )
    parse_params_opts "$@"
    echo "${params[@]@K}"
    echo "${opts[@]@K}"
    # shellcheck disable=SC2068
    set -- ${@/=/ }
    # shellcheck disable=SC2190,SC2089
    if [[ ${opts[force]} == [nN1]* ]]; then
        if [[ ${opts[default]} == [yY0]* ]]; then
            default_prompt="[Y/n]"
        else
            default_prompt="[y/N]"
        fi
        prompt="$("${opts[level]}" "${params[@]}" "${default_prompt}: " )"
        read -rp "$prompt" value
    fi
    value="${value:-${opts[default]}}"
    if [[ $value == [yY0]* ]]; then
        debugn "You selected '$value' (=> 0)"
        return 0
    elif [[ $value == [nN1]* ]]; then
        debugn "You selected '$value' (=> 1)"
        return 1
    fi
    if [[ -z $force ]]; then
        warnn "Please answer either yes or no!"
        "${FUNCNAME[0]}" "$@"
    else
        die "${FUNCNAME[0]} run with force and non-yes/no value (got ${*@K})"
    fi
}