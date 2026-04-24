# ISC License
#
# Copyright (c) 2023-2026, Elia Rizzetto, Silvio Peroni
#
# Permission to use, copy, modify, and/or distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
# INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
# LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
# OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
# PERFORMANCE OF THIS SOFTWARE.

class Semantics:
    """
    Validates the semantic compatibility between identifier schemes and
    resource types in META-CSV rows.
    """

    def __init__(self) -> None:
        """
        Initialise the Semantics checker.

        :rtype: None
        """
        pass

    def check_semantics(self, row: dict, alignment: dict) -> dict:
        """
        Check whether all identifiers in the ``id`` field are compatible with the ``type`` value.

        Uses an alignment dictionary that maps each resource type to the set of
        allowed identifier schemes.

        :param row: A dictionary representing a single CSV row.
        :type row: dict
        :param alignment: Mapping from resource type to the set of accepted ID schemes.
        :type alignment: dict
        :return: Dictionary locating incompatible fields and items, or an empty
            dictionary if no semantic errors were found.
        :rtype: dict
        """
        invalid_row = {}
        row_type = row['type']
        row_ids = row['id'].split(' ')  # list
        invalid_ids_idxs = []

        if row['type'] and row['id']: # apply semantic checks only if both 'id' and 'type' are not empty
            for id_idx, id in enumerate(row_ids):
                if id[:id.index(':')] not in alignment[row_type]:
                    invalid_ids_idxs.append(id_idx)

        if invalid_ids_idxs:
            invalid_row['id'] = invalid_ids_idxs
            invalid_row['type'] = [0]
        return invalid_row
