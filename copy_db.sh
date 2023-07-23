HOME_PREFIX="/var/www/html/mike/DBManager/"
HOME_DATABASE="base.db"

DEST_PREFIX="/var/www/html/mike/GEPSSoundings/"

cp ${HOME_PREFIX}${HOME_DATABASE} ${DEST_PREFIX}${HOME_DATABASE} 2>&1

echo "Script copy_db.sh complete."
