public:
    enum shutdown_mode {

        /**
         * In the write back shutdown mode, all data from the auxiliary chunk and block index is
         * written back to the persistent index. This mode is useful when it it planned to perform
         * a complete replay e.g. with dedupv1_replay after the stop. Usually a writeback stop followed by
         * a replay is faster than a replay alone.
         */
        WRITEBACK,

        /**
         * The fast mode is the default.
         */
        FAST
    };
private:
    /**
     * Shutdown mode
     */
    enum shutdown_mode mode_;
public:

    /**
     * Default constructor using the fast shutdown mode.
     * However, the explicit factory method is preferred.
     */
    StopContext();
    /**
     * Constructor
     * @param mode
     * @return
     */
    explicit StopContext(enum shutdown_mode mode);

    /**
     * returns the shutdown mode
     * @return
     */
    inline enum shutdown_mode mode() const;

    /**
     * Creates a new stop context with a fast shutdown mode.
     * @return
     */
    static StopContext FastStopContext();

    /**
     * Creates a new stop context with the writeback shutdown mode
     */
    static StopContext WritebackStopContext();
private:
    /**
     * Group id to use.
     * If the gid is -1, no special group should be used.
     */
    int gid_;

    /**
     * file mode flags to use
     */
    int mode_;

    /**
     * Constructor.
     *
     * @param gid gid to use, we assume there that the gid is valid.
     * @param dir flag if the file mode is for a directory
     * @param mode permission flags (mode) for the file mode
     * @return
     */
    FileMode(int gid, bool dir, int mode);
public:
    /**
     * Default constructor with no group and read/write permissions
     * for the owner and the group.
     * @return
     */
    FileMode(bool dir = false);

    /**
     * Creates a new file mode given the gid and the mode.
     *
     * @param gid gid to use, we assume there that the gid is valid.
     * @param is_dir flag if the file mode is for a directory
     * @param mode permission flags (mode) for the file mode
     * @return
     */
    static FileMode Create(int gid, bool is_dir, int mode);

    /**
     * Creates a new file mode given the gid and the mode.
     *
     * @param group group name of the group to use
     * @param is_dir flag if the file mode is for a directory
     * @param mode permission flags (mode) for the file mode
     * @return
     */
    static dedupv1::base::Option<FileMode> Create(const std::string& group,
                                                  bool is_dir,
                                                  int mode);

    inline int mode() const;

    inline int gid() const;
public:
    /**
     * Create mode
     */
    enum create_mode {
        NON_CREATE, // !< NON_CREATE
        CREATE,    // !< CREATE
    };

    /**
     * Dirty mode.
     * If a daemon or contrib app is started in a dirty mode, a log
     * replay is necessary to get into a consistent state.
     */
    enum dirty_mode {
        CLEAN, // !< CLEAN
        DIRTY // !< DIRTY
    };

    enum force_mode {
        NO_FORCE,
        FORCE
    };
private:

    /**
     * iff set to true, a class is allowed to create new classes at startup
     */
    enum create_mode create_;

    /**
     * iff set to true, the system has not been shutdown using the writeback approach when it stopped
     * the last time
     */
    enum dirty_mode dirty_;

    /**
     * iff set to true, the system is forced to start. It indicates that situations that are normally
     * consideres as errors should be treated as warnings. If it is possible to repair the state, it should be done.
     */
    enum force_mode force_;

    /**
     * if set to true, all components should be started in a mode that doesn't allow
     * the change of the state.
     */
    bool readonly_;

    /**
     * The file mode is used to specify the permission of all files that are created.
     * The file mode is only used when the create mode is set.
     *
     */
    FileMode file_mode_;

    /**
     * The dir mode is used to specify the permissions of all directories that are created.
     * The dir mode is only used when the create mode is set.
     */
    FileMode dir_mode_;

    /**
     * iff true, the system has not been shutdown correctly the last time it stopped. This clearly indicates
     * a crash.
     */
    bool crashed_;
public:
    /**
     * Constructor.
     *
     * @param create
     * @param dirty
     * @param force
     * @param readonly if true, the object should be started so that so changed are possible
     * @return
     */
    explicit StartContext(enum create_mode create = CREATE,
                          enum dirty_mode dirty = CLEAN,
                          enum force_mode force = NO_FORCE,
                          bool readonly = false);

    /**
     * Returns true iff the start context is in a create mode
     * @return
     */
    inline bool create() const;

    /**
     * Sets the create mode
     * @param c
     * @return
     */
    inline StartContext& set_create(enum create_mode c);

    /**
     * returns true iff the start context is in a dirty mode
     * @return
     */
    inline bool dirty() const;

    /**
     * Sets the dirty mode
     * @param d
     * @return
     */
    inline StartContext& set_dirty(enum dirty_mode d);

    /**
     * returns the force state
     */
    inline bool force() const;

    /**
     * sets the force state
     */
    inline StartContext& set_force(enum force_mode f);

    /**
     * Returns true iff the system should be started in a readonly mode
     * @return
     */
    inline bool readonly() const;

    /**
     * sets the readonly state
     */
    inline StartContext& set_readonly(bool r);

    /**
     * returns the file mode
     */
    inline const FileMode& file_mode() const;

    /**
     * sets the file mode
     */
    inline StartContext& set_file_mode(const FileMode& mode);

    /**
     * returns the dir mode
     */
    inline const FileMode& dir_mode() const;

    /**
     * sets the dir mode
     */
    inline StartContext& set_dir_mode(const FileMode& mode);

    inline bool has_crashed() const;

    inline StartContext& set_crashed(bool crashed);

    std::string DebugString() const;