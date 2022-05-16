class User_Node():
    def __init__(self, user_id, parents_set, childrens_set, level_from_root, shortest_path_from_root, credit):
        self.user_id = user_id or ''
        self.parents_set = parents_set or set()
        self.childrens_set = childrens_set or set()
        self.level_from_root = level_from_root or 0
        self.shortest_paths_from_root = shortest_path_from_root or 0
        self.credit                   = credit or 0