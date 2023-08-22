#!/usr/bin/env bash

# Look for Crisp repo in the dir structure
current_dir=$PWD
repo_name=$(basename $current_dir)
until [[ $repo_name == c3p-* ]] || [[ $current_dir == $HOME ]]
do
current_dir=$(dirname $current_dir)
repo_name=$(basename $current_dir)
done

if [[ $current_dir == $HOME ]]
then
    echo "Can't find the current Crisp repo."
else
    rm -rfv $HOME/.cache/pypoetry/virtualenvs/$repo_name*/lib/python*/site-packages/c3p_* | egrep "/site-packages/[^/]*'$"
    rm -rfv $HOME/.cache/pypoetry/virtualenvs/$repo_name*/src/c3p-* | egrep "/src/[^/]*'$"
    echo "-----"
    echo "Cleared Crisp dependencies from $repo_name virtual environment."
fi

echo "Run poetry update c3p-*"
poetry update c3p-*
poetry install

