<?php

namespace Pantheon\Terminus\Commands\WPMS;

use Consolidation\OutputFormatters\StructuredData\RowsOfFields;
use Pantheon\Terminus\Commands\TerminusCommand;
use Pantheon\Terminus\Exceptions\TerminusException;
use Pantheon\Terminus\Exceptions\TerminusNotFoundException;
use Pantheon\Terminus\Site\SiteAwareInterface;
use Pantheon\Terminus\Site\SiteAwareTrait;
use PDO;
use Symfony\Component\Process\Process;
/**
 * Class ChallengeCommand
 * @package Pantheon\Terminus\Commands\WPMS
 */
class MoveSiteCommand extends TerminusCommand implements SiteAwareInterface
{
    use SiteAwareTrait;

    /**
     * Move a site tenant to another pantheon site/env
     *
     * @authorize
     *
     * @command wpms:move
     * @aliases move-site
     *
     * @param string $source_site_env Site & environment in the format `site-name.env`
     * @param string $target_site_env Site & environment in the format `site-name.env`
     * @param string $site_id site identifier to transfer.

     *
     * @usage <site>.<env> <site>.<env> <site_id> transfers a multisite tenant from one site to another
     */
    public function moveSite($source_site_env, $target_site_env,  $site_id)
    {
        // wake up the environments so that commands won't randomly throw errors.
        $this->wakeEnv($source_site_env);
        $this->wakeEnv($target_site_env);

        $source_connection_attributes = $this->getEnv($source_site_env)->connectionInfo();
        $target_connection_attributes = $this->getEnv($target_site_env)->connectionInfo();
        // note for items to move: sites are in the wp_blogs table. blog_id matches table enumeration.

        // note for rsync step: wp_content/sites/[id]/ is the filesystem for wpms tenants on UIC

        //required validation step: make sure the blog_id at the destination doesn't already exist.

        // just in case: a non 'show tables' way of getting the tables list:
        //$query = $source_db->prepare("SELECT table_name FROM INFORMATION_SCHEMA.TABLES where table_schema='pantheon' and table_name regexp 'wp_[a-z].*'");

        // get the list of tables for the site
        $query = $this->db($source_site_env)->prepare("show tables like :site_table_pattern ");
        $query->execute([':site_table_pattern' => "wp_{$site_id}\_%"]);
        // debug query output
//        $query->debugDumpParams();
        $table_list = $query->fetchAll(PDO::FETCH_COLUMN, 0);

        // rather than pdo export/transfer the data between sites, we'll use the shell to run mysqldump for export, and mysql to import.

        //construct the mysqldump command. This will be a dump|import bash command.
        $dump_command = str_replace('mysql', 'mysqldump --column_statistics=false', $source_connection_attributes['mysql_command'])
            . ' ' . implode(' ', $table_list)
        ;

        $target_connection_attributes['mysql_command']=str_replace("mysql", "mysql -A", $target_connection_attributes['mysql_command']);

        echo("Copying Database tables\r\n");
        ob_start();
        shell_exec("$dump_command 2>&1 | {$target_connection_attributes['mysql_command']}  2>&1");
        ob_end_clean();
        // move the one wp_blogs entry over.
        $query = $this->db($source_site_env)->prepare("select * from wp_blogs where blog_id = ? ");
        $query->execute([$site_id]);

        $blog = $query->fetch(PDO::FETCH_ASSOC);

        $insert_string = "replace into wp_blogs(".implode(',', array_keys($blog)).") "
        . "values(" . str_repeat("?, ", count($blog)-1)."?)";

        $query = $this->db($target_site_env)->prepare($insert_string);
        $query->execute(array_values($blog));

        $this->rsync($source_site_env, $target_site_env, $site_id);
    }

    /**
     * Wake up the environment
     * @param $site_env
     */
    private function wakeEnv($site_env){
        // Some Pantheon environments go to sleep after a certain amount of time, and a sleeping environment
        // will not wake for certain commands, thus generating errors.
        // the goal here is to keep track of envs that have been woken up, and how long ago.

        static $envs;
        // if the environment has been woken up, save some time and don't wake it up again.
        //TODO: add a check for time since last wake
        if(!isset($envs[$site_env])) {
            $envs[$site_env] = time();
            echo("Initializing $site_env \r\n");
            ob_start();
            shell_exec("terminus env:wake $site_env 2>&1");
            ob_end_clean();
        }
    }
    /**
     * Move a site tenant to another pantheon site/env
     *
     * @authorize
     *
     * @command wpms:rsync
     * @aliases wpms:test_rsync
     *
     * @param string $site_env Site & environment in the format `site-name.env`
     * @param string $target_site_env Site & environment in the format `site-name.env`
     * @param string $site_id site identifier to transfer.
     *
     * @usage <site>.<env> <site>.<env> <site_id> transfers a multisite tenant from one site to another
     */
    public function rsync($site_env, string $target_site_env, $site_id) {

        // note for rsync step: wp_content/sites/[id]/ is the filesystem for wpms tenants on UIC
        //download
        $this->rsyncGet($site_env, $site_id);

        // upload the site files
        $this->rsyncPut($target_site_env, $site_id);

        // command to delete source data if needed:
        // rsync -rLvz --size-only --checksum --ipv4 --progress -a --delete -e 'ssh -p 2222' empty_folder/ --temp-dir=~/tmp/ $target_env.$targetSiteUUID@appserver.$target_env.$targetSiteUUID.drush.in:files/remote_folder_to_empty

    }

    /**
     * Move a site tenant to another pantheon site/env
     *
     * @authorize
     *
     * @command wpms:rsync:get
     * @aliases wpms:rget
     *
     * @param string $site_env Site & environment in the format `site-name.env`
     * @param string $site_id site identifier to transfer.
     *
     * @usage <site>.<env> <site>.<env> <site_id> transfers a multisite tenant from one site to another
     */
    public function rsyncGet($site_env, $site_id)
    {
        $this->wakeEnv($site_env);
        $siteUUID = $this->getSite($site_env)->serialize()['id'];
        list($site, $env) = explode(".", $site_env);

        mkdir("/tmp/files/");
        mkdir("/tmp/files/{$site_id}");
        // download the site files

        // in order to show a decent status using rsync < 3.0.0 (OSX ships with 2.6.4)
        // we need to do a dry-run at the source first, save the list, then do the transfer.
//        $generateManifest = new Process("rsync -rvlz --copy-unsafe-links --size-only --checksum --ipv4 --progress -e 'ssh -p 2222' $env.$siteUUID@appserver.$env.$siteUUID.drush.in:files/sites/{$site_id}/ /tmp/files/{$site_id} --dry-run > /tmp/files/manifest.{$site_env}.{$site_id}.txt");
        $generateManifest = new Process(["rsync",
            "-rvlz",
            "--copy-unsafe-links",
            "--size-only",
            "--checksum",
            "--ipv4",
            "--progress",
            "-e",
            "ssh -p 2222",
            "$env.$siteUUID@appserver.$env.$siteUUID.drush.in:files/sites/{$site_id}/",
            "/tmp/files/{$site_id}",
            "--dry-run",
        ]);
$generateManifest->setTimeout(10000000);
        $generateManifest->start();

        // not sure if order matters here, but this does provide output that can be written to a file.

        $manifestFile = fopen("/tmp/files/manifest.{$site_env}.{$site_id}.txt", 'w');
        $generateManifest->wait(function ($type, $buffer) use ($manifestFile) {
            // for this, we need to on-screen the errors, and send output to a file
            if (Process::ERR === $type) {
                echo 'ERR > '.$buffer;
            } else {
                fwrite($manifestFile, $buffer);
            }
        });
        fclose($manifestFile);

        // the entire manifest is here (if needed).
        $manifest = $generateManifest->getOutput();

        /* the options for progress output are:
        1) count the lines in the manifest, then count the lines in the execution
        */

        // the file list can be really big, so use this low memory way to count files
        $handleFile = fopen("/tmp/files/manifest.{$site_env}.{$site_id}.txt", "r");
        $linecount=0;
        while(!feof($handleFile)){
            $line = fgets($handleFile);
            $linecount++;
        }
        fclose($handleFile);


//        exec("rsync -rvlz --copy-unsafe-links --size-only --checksum --ipv4 --progress -e 'ssh -p 2222' $env.$siteUUID@appserver.$env.$siteUUID.drush.in:files/sites/{$site_id}/ /tmp/files/{$site_id} --files-from=/tmp/files/manifest.{$site_env}.{$site_id}.txt\r\n");
        $process = new Process(["rsync",
            "-rvlz",
            "--copy-unsafe-links",
            "--size-only",
            "--checksum",
            "--ipv4",
//            "--progress",
            "-e",
            "ssh -p 2222",
            "--files-from=/tmp/files/manifest.{$site_env}.{$site_id}.txt",
//            "/tmp/files/{$site_id}",
            "$env.$siteUUID@appserver.$env.$siteUUID.drush.in:files/sites/{$site_id}/",
            "/tmp/files/{$site_id}",
        ]);
        $process->setTimeout(10000000);
        $process->start();

        echo("Downloading files from remote\r\n");

        $process->wait(function ($type, $buffer) use (&$linecount) {
            // for this, we need to on-screen the errors, and send output to a file
            if (Process::ERR === $type) {
//                echo 'ERR > '.$buffer;
            } else {
                if($newlines = count(explode("\n", $buffer))-1){
                    $linecount -= $newlines;
                    echo "\rFiles Remaining: $linecount";
                }
            }
        });

        echo("\rFiles Remaining: Complete!\r\n");
    }

    /**
     * Move a site tenant to another pantheon site/env
     *
     * @authorize
     *
     * @command wpms:rsync:put
     * @aliases wpms:rput
     *
     * @param string $site_env Site & environment in the format `site-name.env`
     * @param string $site_id site identifier to transfer.
     *
     * @usage <site>.<env> <site>.<env> <site_id> transfers a multisite tenant from one site to another
     */

    public function rsyncPut($site_env, $site_id)
    {
        $this->wakeEnv($site_env);
        //TODO: validate that files exist in the temp directory
        $siteUUID = $this->getSite($site_env)->serialize()['id'];
        list($site, $env) = explode(".", $site_env);
        $sftp_command = $this->getEnv($site_env)->connectionInfo()['sftp_command'];

        @exec("echo mkdir /files/sites|{$sftp_command}  2>&1");
        @exec("echo mkdir /files/sites/{$site_id}|{$sftp_command} 2>&1");
        // rsync to the target
//        exec("rsync -rLvz --size-only --checksum --ipv4 --progress -e 'ssh -p 2222' /tmp/files/{$site_id}/ --temp-dir=~/tmp/ $env.$siteUUID@appserver.$env.$siteUUID.drush.in:files/sites/{$site_id}/");


        $process = new Process(["rsync",
            "-rvlz",
            "--copy-unsafe-links",
            "--size-only",
            "--checksum",
            "--ipv4",
//            "--progress",
            "-e",
            "ssh -p 2222",
//            "--files-from=/tmp/files/manifest.{$site_env}.{$site_id}.txt",
            "/tmp/files/{$site_id}",
            "$env.$siteUUID@appserver.$env.$siteUUID.drush.in:files/sites/{$site_id}/",
        ]);
        $process->setTimeout(10000000);
        $process->start();

        echo("Uploading files to remote\r\n");
        $linecount = 0;
        $process->wait(function ($type, $buffer) use (&$linecount) {
            // for this, we need to on-screen the errors, and send output to a file
            if (Process::ERR === $type) {
//                echo 'ERR > '.$buffer;
            } else {
                if($newlines = count(explode("\n", $buffer))-1){
                    $linecount += $newlines;
                    echo "\rFiles Uploaded: $linecount";
                }
            }
        });

        echo("\rFiles Uploaded: Complete!\r\n");



    }

    /**
     * Move a site tenant to another pantheon site/env
     *
     * @authorize
     *
     * @command wpms:rsync:delete
     * @aliases wpms:rdel
     *
     * @param string $site_env Site & environment in the format `site-name.env`
     * @param string $site_id site identifier to transfer.
     *
     * @usage <site>.<env> <site>.<env> <site_id> transfers a multisite tenant from one site to another
     */
    public function rsyncDel($site_env, $site_id)
    {
        $siteUUID = $this->getSite($site_env)->serialize()['id'];
        list($site, $env) = explode(".", $site_env);
        $sftp_command = $this->getEnv($site_env)->connectionInfo()['sftp_command'];

        // rsync to the target
        exec("rsync -rLvz --size-only --checksum --ipv4 --progress -a --delete -e 'ssh -p 2222' /dev/null/ --temp-dir=~/tmp/ $env.$siteUUID@appserver.$env.$siteUUID.drush.in:files/sites/{$site_id}");

    }

    /**
     * Initialize an installed site based on the config of an existing site in the upstream
     *
     * @authorize
     *
     * @command wpms:rsync:delete
     * @aliases wpms:rdel
     *
     * @param string $site_env Site & environment in the format `site-name.env`
     * @param string $site_id site identifier to transfer.
     *
     * @usage <site>.<env> <site>.<env> <site_id> transfers a multisite tenant from one site to another
     */

    function WPMSInitialize($site_env, $source_env) {
        //TODO: copy the WP_XYZ tables from source -> target so that the already defined setup is used.
    }

    /**
     * Coordinate the ID assignment between all common multisites
     * @authorize
     *
     * @command wpms:coordinate
     * @aliases wpms:coordinate
     *
     * @param string $upstream upstream that manages the code for the multisites in the set
     * @param string $env environment to coordniate sites
     * @param string $table table to coordinate ID assignment
     *
     * @usage <site>.<env> <site>.<env> <site_id> transfers a multisite tenant from one site to another
     */
    public function coordinate($upstream, $env = 'dev', $table = 'blogs') {

        // collect a list of sites using the upstream, Find the highest blog_id, then set the autoincrement
        // values for the wp_blogs table to iterate in chunks to allow sites to be moved between envs.
        $sites = json_decode(shell_exec("terminus site:list --upstream=$upstream --fields=Name --format=json"));

        // list of subscribers
        foreach($sites as $id=>$site){
            echo("processing: {$site->name}\r\n");
            $this->wakeEnv("{$site->name}.$env");
            $query = $this->db("{$site->name}.$env")->prepare("select max(ID) from wp_blogs");
            $query->execute();

            $max_id = $query->fetchAll(PDO::FETCH_COLUMN, 0);
print_r($max_id);

//            echo("max id is: $max_id");
        }

    }

    private function db($env){
        static $connections;
        if(!isset($connections[$env])){
            $target_env = $this->getEnv($env);

            $target_connection_attributes=$target_env->connectionInfo();

            $target_connection_string =
                'mysql:host='.$target_connection_attributes['mysql_host']
                .';port='.$target_connection_attributes['mysql_port']
                .';dbname='.$target_connection_attributes['mysql_database']
            ;

            $target_db = new PDO($target_connection_string, $target_connection_attributes['mysql_username'], $target_connection_attributes['mysql_password']);
            $target_db->setAttribute( PDO::ATTR_ERRMODE, PDO::ERRMODE_WARNING );
            $connections[$env] = $target_db;
        }

        return $connections[$env];
    }
}
