@echo off
title HsHub Server
color 0B
echo.
echo  ██╗  ██╗███████╗██╗  ██╗██╗   ██╗██████╗
echo  ██║  ██║██╔════╝██║  ██║██║   ██║██╔══██╗
echo  ███████║███████╗███████║██║   ██║██████╔╝
echo  ██╔══██║╚════██║██╔══██║██║   ██║██╔══██╗
echo  ██║  ██║███████║██║  ██║╚██████╔╝██████╔╝
echo  ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝ ╚═════╝ ╚═════╝
echo.
echo  Demarrage du serveur HsHub...
echo  Ouvrez votre navigateur sur : http://localhost:7331
echo.

:: Verifier Node.js
where node >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERREUR] Node.js n'est pas installe.
    echo Telechargez-le sur : https://nodejs.org
    pause
    exit /b 1
)

:: Installer les dependances si absentes
if not exist "node_modules\express\package.json" (
    echo [INFO] Installation des dependances...
    npm install --loglevel=error
    echo.
) else if not exist "node_modules\better-sqlite3\package.json" (
    echo [INFO] Installation de better-sqlite3...
    npm install better-sqlite3 --loglevel=error
    echo.
)

:: Lancer le serveur
node hshub-server.js %*
pause
